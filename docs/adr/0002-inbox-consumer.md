# ADR 0002 — Inbox tüketici: tam bir kez etki

**Durum:** Kabul edildi

## Karar

* Tam bir kez *teslim* yoktur; "bunu yaptım mı?" kaydı ve etki birlikte commit olduğunda tam bir kez *etki* vardır. Tüketici her mesaj için bir PostgreSQL işlemi açar, `courier_inbox`'a `(tüketici grubu, mesaj kimliği)` ekler (`ON CONFLICT DO NOTHING`), satır yeniyse aynı bağlantı ve işlemle handler'ı çalıştırır ve commit eder. Yeniden teslim edilen mesaj inbox satırını bulur ve atlanır.
* **Toplulaştırma:** en fazla `BatchSize` mesaj tüketilir ve tek işlemde işlenir (tek çok satırlı inbox eklemesi, handler'lar sırayla, tek commit). Partide bir şey fırlatırsa işlem (inbox satırları dahil) geri alınır ve mesajlar yeniden deneme yolundan tek tek işlenir. Zehirli mesaj bu yüzden bir ek handler çağrısına mal olur; partisindeki sağlıklı mesajlar etkilenmez.
* **Yeniden deneme ve ölü mektup:** handler `MaxAttempts`'e kadar üstel beklemeyle yeniden denenir; `PermanentFailureException` denemeyi atlar. Sonra mesaj `<topic>.dlq`'ya orijinal topic/partition/offset, tüketici grubu, hata metni ve orijinal başlıklarla üretilir ve offset ilerler; böylece tek kötü mesaj partition'ı bloklamaz. `DeadLetters.ReplayAsync` ölü mektupları orijinal mesaj kimliğiyle orijinal konuya yeniden yayınlar.
* **Offset'ler manueldir.** İşlem commit olduktan sonra saklanır, aralıkla ve partition'lar geri alındığında commit edilir, düzgün durdurmada commit edilip kapatılır. `KillAsync` ikisini de atlar; bir çökme tam olarak böyle görünür ve testler bunu kullanır.
* `message-id` başlığı olmayan yabancı üreticilerin mesajları `topic/partition/offset` ile tanımlanır.
* Rebalance dalgalanmasını sınırlamak için tüketiciler cooperative-sticky'dir.

## Sonuçlar

* Garantinin tutması için handler yalnızca aynı PostgreSQL veritabanına yazmalıdır; diğer yan etkiler kendi idempotency anahtarlarını gerektirir (`message.MessageId` kararlıdır).
* Inbox satırları birikir (README, bilinen sınırlar).
* Çökmeden sonra yeniden teslim normal ve ucuzdur: inbox eklemesinde biter.
