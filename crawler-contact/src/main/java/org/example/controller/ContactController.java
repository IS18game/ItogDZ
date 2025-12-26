package org.example.controller;

import lombok.RequiredArgsConstructor;
import org.example.model.ContactInfo;
import org.example.repository.ContactInfoRepository;
import org.example.service.CrawlerService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api")
public class ContactController {

    private final CrawlerService crawlerService;
    private final ContactInfoRepository repo;

    @PostMapping("/start")
    public ResponseEntity<String> start(@RequestParam("url") String url) {
        crawlerService.start(url);
        return ResponseEntity.ok("Краулер запущен");
    }

    @GetMapping("/contacts")
    public List<ContactInfo> getAll() {
        return repo.findAll();
    }

    @GetMapping(value = "/contacts/text", produces = MediaType.TEXT_PLAIN_VALUE)
    public String getContactsAsText() {
        StringBuilder sb = new StringBuilder();
        for (ContactInfo c : repo.findAll()) {
            if (c.getPhone() != null) {
                sb.append("📞 Телефон: ").append(c.getPhone()).append("\n");
            }
            if (c.getEmail() != null) {
                sb.append("📧 Email: ").append(c.getEmail()).append("\n");
            }
            if (c.getAddress() != null) {
                sb.append("🏠 Адрес: ").append(c.getAddress()).append("\n");
            }
            sb.append("🔗 Источник: ").append(c.getSourceUrl()).append("\n\n");
        }
        return sb.toString();
    }

    @DeleteMapping("/contacts")
    public ResponseEntity<String> deleteAll() {
        repo.deleteAll();
        return ResponseEntity.ok("🧹 Все контакты удалены");
    }

    @GetMapping("/status")
    public ResponseEntity<String> status() {
        long total = repo.count();
        long emails = repo.findAll().stream().filter(c -> c.getEmail() != null).count();
        long phones = repo.findAll().stream().filter(c -> c.getPhone() != null).count();
        long addresses = repo.findAll().stream().filter(c -> c.getAddress() != null).count();

        return ResponseEntity.ok("📊 Всего: " + total +
                "\n📧 Email: " + emails +
                "\n📞 Телефоны: " + phones +
                "\n📍 Адреса: " + addresses);
    }
}