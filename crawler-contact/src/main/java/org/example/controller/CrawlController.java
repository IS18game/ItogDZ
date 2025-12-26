package org.example.controller;

import org.example.service.CrawlerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Set;

@RestController
@RequestMapping("/")
public class CrawlController {
    private final CrawlerService crawler;

    public CrawlController(CrawlerService crawler) {
        this.crawler = crawler;
    }

    @PostMapping("/start")
    public ResponseEntity<String> start(@RequestBody StartRequest req) {
        crawler.submitSeeds(Set.copyOf(req.seeds), req.maxPages, req.maxDepth);
        return ResponseEntity.ok("Crawl started");
    }

    @GetMapping("/answer")
    public ResponseEntity<?> answer(@RequestParam(name = "page", defaultValue = "0") int page,
                                    @RequestParam(name = "size", defaultValue = "20") int size,
                                    @RequestParam(name = "sortBy", defaultValue = "email") String sortBy) {
        return ResponseEntity.ok(crawler.getResults(page, size, sortBy));
    }

    public static class StartRequest {
        public java.util.List<String> seeds;
        public int maxPages = 100;
        public int maxDepth = 2;
    }
}