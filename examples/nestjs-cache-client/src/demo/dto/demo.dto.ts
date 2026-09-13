import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";

export class SetKeyValueDto {
  @ApiProperty({
    description: "Önbelleğe kaydedilecek anahtar (key)",
    example: "session:user_token_99",
  })
  key: string;

  @ApiProperty({
    description: "Önbelleğe kaydedilecek değer (string, number, object vb.)",
    example: { userId: 42, role: "admin" },
  })
  value: any;

  @ApiPropertyOptional({
    description: "Saniye cinsinden geçerlilik süresi (TTL)",
    example: 60,
  })
  ttl?: number;
}

export class CreateUserProfileDto {
  @ApiProperty({
    description: "Kullanıcı benzersiz ID değeri",
    example: "usr_777",
  })
  id: string;

  @ApiProperty({
    description: "Kullanıcı adı ve soyadı",
    example: "Çağdaş",
  })
  name: string;

  @ApiProperty({
    description: "Kullanıcı e-posta adresi",
    example: "cagdas@example.com",
  })
  email: string;

  @ApiProperty({
    description: "Kullanıcı rolü",
    example: "Staff Engineer",
  })
  role: string;

  @ApiProperty({
    description: "Son güncelleme tarihi / zaman damgası",
    example: "2026-09-12",
  })
  updatedAt: string;
}

export class PushTaskDto {
  @ApiProperty({
    description: "Kuyruğa (List) eklenecek görev açıklaması",
    example: "Task #1 - Compile Cache Engine",
    default: "Default Task",
  })
  task: string;
}

export class AddTagsDto {
  @ApiProperty({
    description: "Kümeye (Set) eklenecek tekil etiket dizisi",
    example: ["rust", "nestjs", "distributed-systems"],
    type: [String],
  })
  tags: string[];
}

export class PublishNotificationDto {
  @ApiProperty({
    description: "Pub/Sub kanalına iletilecek bildirim mesajı",
    example: "Sistem güncellemesi tamamlandı!",
    default: "Ping!",
  })
  message: string;
}
