import { NestFactory } from "@nestjs/core";
import { Logger } from "@nestjs/common";
import { DocumentBuilder, SwaggerModule } from "@nestjs/swagger";
import { AppModule } from "./app.module";

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // Uygulama kapatıldığında In-Memory-Cache bağlantılarını temiz sonlandırmak için
  app.enableShutdownHooks();

  // Swagger OpenAPI Dokümantasyonu Kurulumu
  const config = new DocumentBuilder()
    .setTitle("In-Memory Cache NestJS Client API")
    .setDescription(
      "Rust tabanlı In-Memory Cache sunucusu için NestJS istemci servis entegrasyonu ve interaktif API dokümantasyonu.",
    )
    .setVersion("1.0.0")
    .addTag(
      "Cache Demo",
      "Strings, Hashes, Lists, Sets ve Pub/Sub operasyonları",
    )
    .build();

  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup("api", app, document);

  const port = process.env.PORT || 3000;
  await app.listen(port);

  Logger.log(
    `🚀 NestJS Cache Client API is running on http://localhost:${port}`,
    "Bootstrap",
  );
  Logger.log(
    `📚 Swagger UI is available at http://localhost:${port}/api`,
    "Bootstrap",
  );
  Logger.log(
    `🔗 Connected to Rust In-Memory Cache on ${process.env.CACHE_HOST || "127.0.0.1"}:${process.env.CACHE_PORT || 6379}`,
    "Bootstrap",
  );
}

bootstrap();
