import { Module } from "@nestjs/common";
import { InMemoryCacheModule } from "./cache";
import { DemoModule } from "./demo/demo.module";

@Module({
  imports: [
    // Global Cache Modülümüzü Rust sunucu parametreleriyle başlatıyoruz
    InMemoryCacheModule.forRoot({
      host: process.env.CACHE_HOST || "127.0.0.1",
      port: Number(process.env.CACHE_PORT) || 6379,
      enablePubSub: true,
    }),
    DemoModule,
  ],
})
export class AppModule {}
