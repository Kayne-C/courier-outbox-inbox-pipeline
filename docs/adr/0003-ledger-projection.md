# ADR 0003 — Defter örneği: hattı sürümlerle kanıtlamak

**Durum:** Kabul edildi

## Karar

* Doğruluk kaynağı `ledger_accounts`'tur (bakiye ve her değişiklikte artan bir **sürüm**). Transfer tek işlemdir: transfer kimliğini kaydet (birincil anahtar yeniden denemeleri idempotent yapar), iki hesabı id sırasıyla kilitle (deadlock yok), bakiyeyi denetle, ikisini güncelle ve hesap başına bir olay olmak üzere iki olayı enqueue et; anahtar hesaptır ve yeni sürümü taşır.
* Transfer başına değil hesap başına olay, Kafka anahtarını sırası önemli olan varlıkla eşit tutar.
* Projektör bir olayı yalnızca `applied_version = event.version − 1` ise uygular (tek `UPDATE … WHERE`); 1. sürüm satırı ekler. Eski sürüm durumca tekrardır ve yok sayılır; boşluk hatadır (yeniden denenir, sonra ölü mektuba gider), sessizce atlanmaz.
* Bu, hattın her garantisini bir testin doğrulayabileceği hale getirir: çökmelerden sonra projeksiyon kaynakla **bakiye ve sürümde** eşleşmeli, inbox tam `2 × transfer + hesap` satır içermeli ve para korunmalı.
* Hesap açma da bir olaydır (`version 1`); böylece projeksiyon yalnızca konudan yeniden kurulabilir.

## Sonuçlar

* Hesap başına tek olay akışı, yavaş bir hesabın başka birinin sırasını bozmamasını sağlar.
* Projeksiyonu yeniden kurmak, konuyu yeni bir tüketici grubuyla tekrar oynatmayı gerektirir (inbox grup başınadır).
