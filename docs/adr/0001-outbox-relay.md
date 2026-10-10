# ADR 0001 — Shard'lı, seri relay ile işlemsel outbox

**Durum:** Kabul edildi

## Bağlam

Veritabanına yazmak ve Kafka'ya yayınlamak tek atomik işlem olamaz. İkisini art arda yapmak olayları kaybeder (arada çökme) ya da geri alınmış durum için olay yayınlar.

## Karar

* Olay, `courier_outbox` tablosuna **çağıranın işlemi içinde** yazılır. Commit mantıksal olarak yayınlar, geri alma atar.
* Relay bekleyen satırları okur, üretir ve yalnızca broker onayladıktan sonra yayınlandı işaretler. Yayın bu yüzden **en az bir kez**dir: onaydan sonra çökme partiyi yeniden yayınlar.
* **Sıralama:** her satırın `shard = FNV-1a(key) % 16` değeri vardır (kararlı hash; `string.GetHashCode` süreç başına rastgele olduğundan yeniden başlatmalarda sırayı bozardı). Bir shard aynı anda yalnızca bir relay tarafından id sırasıyla boşaltılır. Relay, shard başına adanmış bir bağlantıda oturum düzeyinde advisory kilit tutar; bağlantı ya da süreç ölürse PostgreSQL kilidi bırakır ve başka relay devralır. Bir relay aldığı shard sayısını sınırlayabilir; böylece ikinci bir örnek yükü paylaşabilir.
* Üretici idempotent ve `acks=all`; bu, iç yeniden denemelerde sırayı korur. Bir partideki bir mesaj başarısız olursa aynı partition'daki sonrakiler de başarısız olur; teslim edilen küme önektir ve yalnızca onaylanan id'ler işaretlenir.
* Satırlar kararlı bir `message_id` taşır (enqueue anında atanan GUID); header olarak gönderilir ve aşağı akımda tekilleştirme kimliğidir.
* `published_at` dolu satırlar saklanır (denetim, hata ayıklama); bekleyenler için kısmi indeks küçük kalır.
* Bir test kancası, `AfterProduceBeforeMark`, testlerin relay'i önemli olan tek pencerede çökertmesini sağlar.

## Sonuçlar

* Tekrarlar beklenir ve tüketici tarafından yutulmalıdır (ADR 2).
* Bir shard'ın verimi tek seri okuyucudur; paralellik 16 shard'dan gelir.
* İki relay aynı shard'ı eşzamanlı yayınlarsa sıra garantisinin belgelenmiş bir boşluğu vardır (README, bilinen sınırlar).
