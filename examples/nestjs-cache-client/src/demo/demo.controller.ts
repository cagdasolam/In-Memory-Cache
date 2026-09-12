import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  NotFoundException,
} from "@nestjs/common";
import { DemoService, UserProfile } from "./demo.service";

@Controller("demo")
export class DemoController {
  constructor(private readonly demoService: DemoService) {}

  // 1. Strings & TTL
  @Post("kv")
  async setKeyValue(@Body() body: { key: string; value: any; ttl?: number }) {
    return this.demoService.setKeyValue(body.key, body.value, body.ttl);
  }

  @Get("kv/:key")
  async getKeyValue(@Param("key") key: string) {
    const res = await this.demoService.getKeyValue(key);
    if (res.value === null) {
      throw new NotFoundException(`Key '${key}' not found in cache.`);
    }
    return res;
  }

  // 2. Hashes (User Profiles)
  @Post("users")
  async saveUserProfile(@Body() profile: UserProfile) {
    return this.demoService.saveUserProfile(profile);
  }

  @Get("users/:id")
  async getUserProfile(@Param("id") id: string) {
    const profile = await this.demoService.getUserProfile(id);
    if (!profile) {
      throw new NotFoundException(`User '${id}' not found in cache.`);
    }
    return profile;
  }

  // 3. Lists (Task Queue)
  @Post("tasks")
  async pushTask(@Body() body: { task: string }) {
    return this.demoService.pushTask(body.task || "Default Task");
  }

  @Get("tasks")
  async getTasks() {
    return this.demoService.getTasks();
  }

  // 4. Sets (Unique Tags)
  @Post("tags")
  async addTags(@Body() body: { tags: string[] }) {
    return this.demoService.addTags(body.tags || []);
  }

  @Get("tags")
  async getTags() {
    return this.demoService.getTags();
  }

  // 5. Pub/Sub (Real-time Broadcast)
  @Post("publish")
  async publishNotification(@Body() body: { message: string }) {
    return this.demoService.sendNotification(body.message || "Ping!");
  }
}
