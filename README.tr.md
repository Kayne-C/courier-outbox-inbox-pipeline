# Courier

[English](README.md) · [Türkçe](README.tr.md)

.NET, PostgreSQL ve Kafka uyumlu herhangi bir broker (Redpanda ile test edildi) için işlemsel **outbox → Kafka → idempotent inbox** hattı. İki klasik sorunu çözer: çift yazma (bir veritabanı değişikliği ve olayı birlikte gerçekleşmeli) ve tekrar teslim (broker en az bir kez teslim eder). Küçük, test edilmiş bir kütüphane ve garantileri hata altında kanıtlayan örnek bir defter (ledger) servisinden oluşur.

```
 işleminiz ──► durum + outbox satırı ──commit──► relay ──► Kafka ──► tüketici ──► inbox satırı + handler etkisi ──commit
                                                  ▲ en az bir kez, anahtar başına sıralı │   (tek işlem: etki bir kez gerçekleşir)
```

## İçerik

| Yetenek | Nasıl |
|---|---|
| Atomik durum + olay | `Outbox.EnqueueAsync(connection, tx, message)` olayı *sizin* işleminizde yazar; geri alma hiçbir şey yayınlamaz |
| Anahtar başına sıralama | Outbox satırları anahtarın kararlı bir hash'iyle parçalanır (shard); her shard tam bir relay tarafından seri boşaltılır (oturum advisory kilidi), üretici idempotent ve `acks=all` |
| Birden çok relay | Shard'lar `pg_try_advisory_lock` ile kiralanır; ölen relay bağlantısıyla birlikte shard'larını bırakır |
| En az bir kez yayın | Satırlar yalnızca broker onayından sonra yayınlandı işaretlenir; arada çökme yeniden yayınlar |
| Tam bir kez *etki* | Tüketici inbox kaydını ve handler'ınızı **tek işlemde** çalıştırır; yeniden teslim edilen mesaj inbox anahtarına çarpar ve atlanır |
| Toplu tüketim | İşlem başına 100 mesaja kadar; biri başarısız olursa mesaj mesaj işlemeye otomatik geri dönüş |
| Yeniden deneme, ölü mektup, tekrar oynatma | Beklemeli denemeler, `PermanentFailureException` denemeyi atlar, hatalar `<topic>.dlq`'ya hata/kaynak başlıklarıyla gider, `DeadLetters.ReplayAsync` düzeltmeden sonra yeniden yayınlar |
| Offset'ler | Manuel: işlem commit olduktan sonra saklanır, aralıkla ve rebalance'ta commit edilir |

## Tasarım

* **Outbox satırı, tarif ettiği durumla birlikte commit edilir** ([ADR 1](docs/adr/0001-outbox-relay.md)). Relay ayrı bir adımdır: bir shard'ın bekleyen satırlarını id sırasıyla okur, üretir, flush eder, sonra onaylanan id'leri işaretler.
* **Sıralama şanstan değil shard'lamadan gelir.** Aynı anahtar → aynı shard → tek relay tarafından id sırasıyla boşaltılır → aynı Kafka partition'ı → tek tüketici tarafından sırayla tüketilir. Idempotent üretim yeniden denemelerde sırayı korur; bir mesaj başarısız olursa partition'daki sonrakiler de başarısız olur, yani teslim edilen küme her zaman bir önektir.
* **Inbox handler etkisini idempotent yapar** ([ADR 2](docs/adr/0002-inbox-consumer.md)). Tam bir kez *teslim* yoktur; tekilleştirme kaydı ve etki aynı işlemi paylaştığında tam bir kez *etki* vardır.
* **Hata bir partition'ı asla bloklamaz.** Deneme bütçesinden sonra mesaj DLQ konusuna taşınır ve offset ilerler.

## Örnek: okuma modelli bir defter

`Courier.Ledger` küçük bir servistir: PostgreSQL'de hesaplar ve transferler (doğruluk kaynağı) ve yalnızca olaylardan kurulan `view_balances` projeksiyonu. Her transfer iki olay (borç ve alacak) üretir; her biri hesabın yeni **sürümünü** taşır. Projektör bir hesabın olaylarını yalnızca sürüm sırasıyla uygular, boşlukta yüksek sesle hata verir. Böylece hattın garantileri gözlemlenebilir olur: istenen kadar çökmeden sonra `view_balances`, sürümler dahil, `ledger_accounts` ile tam eşit olmalıdır.

## Kanıtlar

Tek Windows makinesi; Docker'da PostgreSQL 17 ve Redpanda 24.3 aynı makinede; her şey tek süreçte. Mutlak değil, göreli okuyun.

| Çalışma | Sonuç |
|---|---|
| 16 test (aşağıda) | CI'da geçer |
| 20.000 transfer (40.500 olay), 8 yazıcı | kaynakta 911 transfer/sn (p50 8,5 ms, p99 14,9 ms); projeksiyon son yazmadan 0,3 sn sonra tamamen yetişti; farklı hesap sayısı 0 |
| Aynısı, önce birikmiş iş: relay ve 3 tüketici 40.500 kuyruktaki olay üzerinde soğuk başlatıldı | 15,4 sn'de boşaldı (~2.600 olay/sn, tüketici grubuna katılım dahil) |

Yeniden üretmek için: `dotnet run -c Release --project bench/Courier.Bench -- --transfers 20000` (birikmiş iş koşusu için `--drain true`).

### Nasıl test edildi (gerçek PostgreSQL + gerçek Redpanda)

* Outbox id'si mesaj kimliği olarak uçtan uca teslim; geri alınan işlem hiçbir şey yayınlamaz.
* **Relay'in "broker onayladı" ile "yayınlandı işaretlendi" arasında çökmesi:** üç kez enjekte edilir; tekrarlar tüketiciye ulaşır ve inbox hepsini yutar (test, tekrarların gerçekten görüldüğünü doğrular).
* **Sıralama:** eşzamanlı yazılan 20 anahtar × 60 ardışık mesaj, aynı grupta 2 relay ve 2 tüketici; handler sıra dışı her teslimi kaydeder: sıfır.
* **Offset'leri commit edilmemiş tüketicinin öldürülmesi** (gerçek kill: commit yok, close yok); yerine geçen yeniden okur ve inbox etki sayısını 300'de tam tutar.
* Geçici hata ölü mektuba gitmeden yeniden denenir; zehirli mesaj deneme bütçesinden sonra sonraki mesajları bloklamadan ölü mektuba gider; kalıcı hata denemeleri atlar; ölü mektuplar "hata düzeltildikten" sonra tekrar oynatılır.
* **Kaos:** 24 hesap arasında 1.500 eşzamanlı transfer (bazıları bakiye yetersizliği), relay'ler durdurulup yeniden başlatılırken, tüketiciler 10 kez öldürülüp yeniden başlatılırken ve relay partilerin %4'ünde işaretlemeden önce rastgele "çökerken". Sonunda projeksiyon her hesap için kaynağa eşit (bakiye ve sürüm), para korunmuş, yayınlanmamış bir şey kalmamış ve inbox tam `2 × transfer + hesap` satır içerir.
* Defter kuralları (bakiye yetersizliği hiçbir şeyi değiştirmez ve olay üretmez, 10 eşzamanlı denemede aynı transfer kimliği parayı bir kez taşır) ve HTTP API.

### Yazarken bulunan hatalar

* "Öldürülen" tüketici close sırasında yine de offset'lerini commit ediyordu; bu yüzden test yeniden teslimi hiç sınamıyordu. Offset commit'leri artık manuel ve `KillAsync` son commit ile close'u atlar; test artık gerçek yeniden teslimi görür.
* İlk toplu projektörüm, `WHERE` ifadesi conflict maddesinin ihtiyaç duyduğu satırı eleyen bir `INSERT … WHERE … ON CONFLICT` upsert kullandı; hesap başına ilk olaydan sonraki her olay boşluk sayılıp ölü mektuba gitti. Kıyaslamanın hiç bitmemesiyle bulundu; yerine `UPDATE … WHERE applied_version = v − 1` ve yalnızca 1. sürüm için insert getirildi.
* Projenin ilk adı Windows'un derleme çıktısına yazmayı reddetmesine yol açtı (muhtemelen DLL adını eşleştiren bir güvenlik ürünü); yeniden adlandırıldı.

## Çalıştırma

```bash
docker compose up --build
KEY="Authorization: Bearer change-me-0123456789"
curl -X PUT localhost:8080/accounts/alice -H "$KEY" -H "Content-Type: application/json" -d '{"balance":500}'
curl -X PUT localhost:8080/accounts/bob   -H "$KEY" -H "Content-Type: application/json" -d '{"balance":0}'
curl -X POST localhost:8080/transfers -H "$KEY" -H "Content-Type: application/json" -d '{"id":"t1","from":"alice","to":"bob","amount":120}'
curl localhost:8080/view/bob -H "$KEY"
```

Kütüphane kullanımı:

```csharp
await using var tx = await connection.BeginTransactionAsync();
// ... durum değişiklikleriniz ...
await Outbox.EnqueueAsync(connection, tx, new OutboxMessage("orders.events", orderId, json));
await tx.CommitAsync();

new OutboxRelay(dataSource, new RelayOptions { BootstrapServers = bootstrap }).Start();
new InboxConsumer(dataSource, new ConsumerOptions { BootstrapServers = bootstrap, GroupId = "billing", Topics = ["orders.events"] },
    async (message, connection, tx, ct) => { /* etkiniz, tx içinde */ }).Start();
```

```bash
dotnet test --solution Courier.slnx
```

## Bilinen sınırlar

* **Anahtar başına sıra yalnızca shard'ı tek relay tuttuğu sürece geçerlidir.** Bir relay, üretici partisi hâlâ uçuştayken veritabanı bağlantısını kaybedecek kadar takılırsa ikinci bir relay aynı satırları yayınlayabilir ve broker bunları birinciye göre sıra dışı görebilir. Inbox etkileri doğru tutar ama katı sıra isteyen handler'lar (örneğin örnekteki sürüm denetimi) bayat olanı ölü mektuba gönderir. Üretim yolunda fencing token yoktur.
* **Rebalance yarışları:** tüketici rebalance'ı sırasında aynı partition kısa süre iki tüketici tarafından işlenebilir; inbox çift etkiyi önler, o andaki sıra en iyi çabadır.
* **Yeniden denemeler partition'ı** ölü mektuptan önce deneme bütçesi kadar (varsayılan yaklaşık 0,7 sn) **bloklar;** bloklamayan retry konuları yoktur.
* **Inbox ve outbox sonsuza dek büyür.** Yayınlanmış outbox satırları veya eski inbox satırları için saklama (retention) işi yoktur (konu saklama süresinden eski bir inbox satırı güvenle silinebilir ama bu uygulanmadı).
* **Tek zehirli mesaj içeren parti** önce parti olarak, sonra mesaj mesaj denenir; zehirli handler `MaxAttempts + 1` kez çağrılır.
* **Yoklamalı relay,** 20 ms boşta aralığı, `LISTEN/NOTIFY` yok; gecikme tabanı bu aralık artı `linger.ms`'dir.
* **Handler ve inbox aynı PostgreSQL veritabanında olmalıdır;** başka sistemlerdeki etkiler kendi idempotency anahtarlarını gerektirir.
* **Mesaj şeması/sözleşme doğrulaması handler'ın işidir;** schema registry entegrasyonu yok.
* Örnek serviste tek paylaşılan API anahtarı. Broker kimlik doğrulaması (SASL/TLS) relay ve tüketici seçeneklerindeki `Configure` geri çağrısıyla geçirilir ve testlerle kapsanmamıştır.

Tasarım kararları: [docs/adr](docs/adr).
