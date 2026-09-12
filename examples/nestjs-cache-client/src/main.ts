import { NestFactory } from "@nestjs/core";
import { Logger } from "@nestjs/common";
import { AppModule } from "./app.module";

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // Uygulama kapatıldığında In-Memory-Cache bağlantılarını temiz sonlandırmak için
  app.enableShutdownHooks();

  const port = process.env.PORT || 3000;
  await app.listen(port);

  Logger.log(
    `🚀 NestJS Cache Client API is running on http://localhost:${port}`,
    "Bootstrap",
  );
  Logger.log(
    `🔗 Connected to Rust In-Memory Cache on ${process.env.CACHE_HOST || "127.0.0.1"}:${process.env.CACHE_PORT || 6379}`,
    "Bootstrap",
  );
}

bootstrap();
