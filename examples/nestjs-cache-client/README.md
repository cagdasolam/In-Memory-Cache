# NestJS In-Memory-Cache Client & Service Integration

Bu proje, geliştirdiğimiz yüksek başarımlı Rust tabanlı In-Memory Cache sunucusunu bir NestJS uygulamasında merkezi ve tip güvenli bir servis (`InMemoryCacheService`) olarak nasıl kullanabileceğinizi gösteren uçtan uca çalışan örnek bir projedir.

---

## 🌟 Özellikler

1. **Tam Veri Yapısı Desteği:**
   - **Strings / Key-Value:** `get<T>()`, `set()`, `del()`, `exists()`
   - **TTL & Expiration:** `ttl()`, `expire()`, `set(..., ttlSeconds)`
   - **Lists (Kuyruk ve Loglama):** `lpush()`, `rpush()`, `lrange()`, `lpop()`
   - **Hashes (Kullanıcı Profilleri / Nesneler):** `hset()`, `hmset()`, `hget()`, `hgetall()`
   - **Sets (Tekil Etiketler / Gruplar):** `sadd()`, `sismember()`, `smembers()`
   - **Pub/Sub (Gerçek Zamanlı Mesajlaşma):** `publish()`, `subscribe()`
2. **Global & Modüler DI:** `@Global()` ile tanımlanmış `InMemoryCacheModule`, projenizdeki herhangi bir servise `InMemoryCacheService` enjekte etmenizi sağlar.
3. **Graceful Shutdown:** NestJS uygulaması sonlandığında (`SIGTERM`/`SIGINT`) TCP soket bağlantılarını sunucuya nazikçe kapatır (`app.enableShutdownHooks()`).

---

## 🚀 Hızlı Başlangıç

### 1. Rust In-Memory Cache Sunucusunu Başlatın
Ana proje dizininde (`/mnt/c/cagdas/projects/In-Memory-Cache`):

```bash
cargo run --release
```
Sunucu varsayılan olarak `127.0.0.1:6379` üzerinde dinlemeye başlar.

### 2. NestJS Bağımlılıklarını Yükleyin ve Başlatın
`examples/nestjs-cache-client` dizininde:

```bash
cd examples/nestjs-cache-client
npm install
npm run start:dev
```

NestJS uygulaması `http://localhost:3000` üzerinde açılacaktır.

---

## 📖 Kendi Servislerinizde Nasıl Kullanırsınız?

### 1. Modülü `AppModule`'e Dahil Edin

```typescript
import { Module } from '@nestjs/common';
import { InMemoryCacheModule } from './cache';

@Module({
  imports: [
    InMemoryCacheModule.forRoot({
      host: '127.0.0.1',
      port: 6379,
      enablePubSub: true, // Pub/Sub dinleyicisi için ayrı bir bağlantı açar
    }),
  ],
})
export class AppModule {}
```

### 2. Servisinizde `InMemoryCacheService` Enjekte Edin

```typescript
import { Injectable } from '@nestjs/common';
import { InMemoryCacheService } from '../cache';

@Injectable()
export class UsersService {
  constructor(private readonly cache: InMemoryCacheService) {}

  async getUser(id: string) {
    // 1. Önce önbelleğe bak
    const cached = await this.cache.get(`user:${id}`);
    if (cached) return cached;

    // 2. Önbellekte yoksa veritabanından getir (örnek veri)
    const userFromDb = { id, name: 'Çağdaş', role: 'Engineer' };

    // 3. 60 saniye TTL ile önbelleğe yaz
    await this.cache.set(`user:${id}`, userFromDb, 60);

    return userFromDb;
  }
}
```

---

## 🧪 Örnek REST API Uç Noktaları ve Test Komutları

Uygulama ile birlikte gelen `DemoController` aşağıdaki uç noktaları sağlar:

### 1. Key-Value & TTL Testi
```bash
# Değer yaz (10 saniye TTL ile)
curl -X POST http://localhost:3000/demo/kv \
  -H "Content-Type: application/json" \
  -d '{"key": "session:token_abc", "value": {"userId": "42", "role": "admin"}, "ttl": 10}'

# Değeri ve kalan süreyi oku
curl http://localhost:3000/demo/kv/session:token_abc
```

### 2. Hash (Kullanıcı Profili) Testi
```bash
# Hash olarak kullanıcı profili kaydet
curl -X POST http://localhost:3000/demo/users \
  -H "Content-Type: application/json" \
  -d '{"id": "usr_101", "name": "Çağdaş", "email": "cagdas@example.com", "role": "Lead Architect", "updatedAt": "2026-09-12"}'

# Kullanıcı profilini oku
curl http://localhost:3000/demo/users/usr_101
```

### 3. List (Görev Kuyruğu) Testi
```bash
# Kuyruğa yeni görevler ekle
curl -X POST http://localhost:3000/demo/tasks \
  -H "Content-Type: application/json" \
  -d '{"task": "Process image #108"}'

# Kuyruktaki görevleri listele
curl http://localhost:3000/demo/tasks
```

### 4. Set (Tekil Etiketler) Testi
```bash
# Etiketleri kümeye ekle
curl -X POST http://localhost:3000/demo/tags \
  -H "Content-Type: application/json" \
  -d '{"tags": ["rust", "nestjs", "distributed-systems", "rust"]}'

# Tüm tekil etiketleri getir
curl http://localhost:3000/demo/tags
```

### 5. Pub/Sub (Gerçek Zamanlı Bildirim) Testi
```bash
# Bildirim yayınla (NestJS konsolunda anında log çıktısı görünür)
curl -X POST http://localhost:3000/demo/publish \
  -H "Content-Type: application/json" \
  -d '{"message": "Sistem güncellemesi tamamlandı!"}'
```

