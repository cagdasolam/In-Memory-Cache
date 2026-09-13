import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  NotFoundException,
} from "@nestjs/common";
import { ApiTags, ApiOperation, ApiParam, ApiResponse } from "@nestjs/swagger";
import { DemoService } from "./demo.service";
import {
  SetKeyValueDto,
  CreateUserProfileDto,
  PushTaskDto,
  AddTagsDto,
  PublishNotificationDto,
} from "./dto/demo.dto";

@ApiTags("Cache Demo")
@Controller("demo")
export class DemoController {
  constructor(private readonly demoService: DemoService) {}

  // 1. Strings & TTL
  @Post("kv")
  @ApiOperation({
    summary: "String Key-Value ve opsiyonel TTL kaydet",
    description:
      "Belirtilen anahtara değer atar ve varsa TTL süresini ayarlar.",
  })
  @ApiResponse({
    status: 201,
    description: "Anahtar ve değer başarıyla kaydedildi.",
  })
  async setKeyValue(@Body() body: SetKeyValueDto) {
    return this.demoService.setKeyValue(body.key, body.value, body.ttl);
  }

  @Get("kv/:key")
  @ApiOperation({
    summary: "String anahtar değerini ve kalan TTL süresini getir",
    description:
      "Önbellekte saklanan anahtarın değerini ve saniye cinsinden kalan süresini döner.",
  })
  @ApiParam({
    name: "key",
    description: "Sorgulanacak anahtar adı",
    example: "session:user_token_99",
  })
  @ApiResponse({
    status: 200,
    description: "Anahtar değeri ve kalan TTL süresi.",
  })
  @ApiResponse({ status: 404, description: "Anahtar önbellekte bulunamadı." })
  async getKeyValue(@Param("key") key: string) {
    const res = await this.demoService.getKeyValue(key);
    if (res.value === null) {
      throw new NotFoundException(`Key '${key}' not found in cache.`);
    }
    return res;
  }

  // 2. Hashes (User Profiles)
  @Post("users")
  @ApiOperation({
    summary: "Hash yapısında Kullanıcı Profili kaydet (HMSET / HSET)",
    description:
      "Kullanıcı profilini hash olarak saklar ve otomatik 1 saatlik TTL tanımlar.",
  })
  @ApiResponse({
    status: 201,
    description: "Kullanıcı profili hash olarak kaydedildi.",
  })
  async saveUserProfile(@Body() profile: CreateUserProfileDto) {
    return this.demoService.saveUserProfile(profile);
  }

  @Get("users/:id")
  @ApiOperation({
    summary: "Kullanıcı Profilini getir (HGETALL)",
    description:
      "Hash yapısında saklanan kullanıcı profilinin tüm alanlarını getirir.",
  })
  @ApiParam({ name: "id", description: "Kullanıcı ID'si", example: "usr_777" })
  @ApiResponse({ status: 200, description: "Kullanıcı profili detayları." })
  @ApiResponse({ status: 404, description: "Kullanıcı önbellekte bulunamadı." })
  async getUserProfile(@Param("id") id: string) {
    const profile = await this.demoService.getUserProfile(id);
    if (!profile) {
      throw new NotFoundException(`User '${id}' not found in cache.`);
    }
    return profile;
  }

  // 3. Lists (Task Queue)
  @Post("tasks")
  @ApiOperation({
    summary: "Görev kuyruğuna (List) yeni görev ekle (RPUSH)",
    description: "queue:tasks listesinin sonuna yeni görev ekler.",
  })
  @ApiResponse({ status: 201, description: "Görev kuyruğa eklendi." })
  async pushTask(@Body() body: PushTaskDto) {
    return this.demoService.pushTask(body.task || "Default Task");
  }

  @Get("tasks")
  @ApiOperation({
    summary: "Tüm kuyruk görevlerini listele (LRANGE)",
    description: "queue:tasks listesindeki tüm kayıtları sırayla döner.",
  })
  @ApiResponse({ status: 200, description: "Kuyruktaki görev listesi." })
  async getTasks() {
    return this.demoService.getTasks();
  }

  // 4. Sets (Unique Tags)
  @Post("tags")
  @ApiOperation({
    summary: "Kümeye (Set) tekil etiketler ekle (SADD)",
    description:
      "system:tags kümesine etiketleri ekler (çift kopyalar otomatik filtrelenir).",
  })
  @ApiResponse({ status: 201, description: "Etiketler kümeye eklendi." })
  async addTags(@Body() body: AddTagsDto) {
    return this.demoService.addTags(body.tags || []);
  }

  @Get("tags")
  @ApiOperation({
    summary: "Tüm tekil etiketleri getir (SMEMBERS)",
    description: "system:tags kümesindeki tüm benzersiz elemanları listeler.",
  })
  @ApiResponse({ status: 200, description: "Tekil etiketler kümesi." })
  async getTags() {
    return this.demoService.getTags();
  }

  // 5. Pub/Sub (Real-time Broadcast)
  @Post("publish")
  @ApiOperation({
    summary: "Pub/Sub kanalında bildirim yayınla (PUBLISH)",
    description: "'app:notifications' kanalına gerçek zamanlı mesaj yayınlar.",
  })
  @ApiResponse({ status: 201, description: "Mesaj kanala başarıyla iletildi." })
  async publishNotification(@Body() body: PublishNotificationDto) {
    return this.demoService.sendNotification(body.message || "Ping!");
  }
}
