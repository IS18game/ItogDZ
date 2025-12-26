package org.example.service;

import org.example.model.ContactInfo;
import org.example.repository.ContactInfoRepository;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class CrawlerService {
    private final ExecutorService executor;
    private final ContactInfoRepository repo;
    private final RestTemplate restTemplate;

    private final Set<String> visited = ConcurrentHashMap.newKeySet();
    private final ConcurrentLinkedQueue<String> frontier = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pagesProcessed = new AtomicInteger(0);

    private static final Pattern EMAIL = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+");
    private static final Pattern PHONE = Pattern.compile("\\+?\\d[\\d\\s().-]{7,}\\d");
    private static final Pattern ADDRESS = Pattern.compile(
            "\\b(\\d{6},?\\s*)?(г\\.?\\s*)?[А-Яа-яёЁ\\-\\s]+,?\\s*(ул\\.|улица|проспект|пр\\.|пер\\.|переулок|наб\\.|набережная|шоссе|ш\\.|бульвар|бул\\.|пл\\.|площадь)\\s+[А-Яа-яёЁ\\-\\s]+,?\\s*(д\\.|дом)?\\s*\\d+[А-Яа-я]?(\\s*корп\\.?\\s*\\d+)?"
    );

    public CrawlerService(@Qualifier("crawlerExecutor") ExecutorService executor,
                          ContactInfoRepository repo,
                          RestTemplate restTemplate) {
        this.executor = executor;
        this.repo = repo;
        this.restTemplate = restTemplate;
    }

    public void submitSeeds(Set<String> seeds, int maxPages, int maxDepth) {
        frontier.addAll(seeds);
        visited.addAll(seeds);
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            executor.submit(() -> crawlLoop(maxPages, maxDepth));
        }
    }

    public void start(String url) {
        Set<String> seeds = Set.of(url);
        int maxPages = 100; // или сколько нужно
        int maxDepth = 3;   // если используешь глубину

        submitSeeds(seeds, maxPages, maxDepth);
    }

    private void crawlLoop(int maxPages, int maxDepth) {
        while (pagesProcessed.get() < maxPages) {
            String url = frontier.poll();
            if (url == null) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                continue;
            }
            try {
                processUrl(url);
                if (pagesProcessed.incrementAndGet() >= maxPages) break;
            } catch (Exception ignored) {}
        }
    }

    private void processUrl(String url) {
        System.out.println("➡️ Processing: " + url);
        try {
            String html = restTemplate.getForObject(URI.create(url), String.class);
            if (html == null || html.isBlank()) {
                System.out.println("⚠️ Empty or null HTML for: " + url);
                return;
            }

            System.out.println("✅ HTML loaded (" + html.length() + " chars)");

            Document doc = Jsoup.parse(html, url);
            String text = doc.text();

            Set<String> seenEmails = new HashSet<>();

            Matcher mEmail = EMAIL.matcher(text);
            while (mEmail.find()) {
                String email = mEmail.group().toLowerCase().trim();

                if (!email.isBlank() &&
                        seenEmails.add(email) &&
                        !repo.existsBySourceUrlAndEmail(url, email)) {

                    ContactInfo info = new ContactInfo();
                    info.setSourceUrl(url);
                    info.setEmail(email);
                    repo.save(info);
                    System.out.println("📧 Найдена эл. почта: " + email);
                }
            }

            Set<String> seenPhones = new HashSet<>();

            Matcher mPhone = PHONE.matcher(text);
            while (mPhone.find()) {
                String phone = mPhone.group();
                String digits = phone.replaceAll("\\D", "");
                String normalized = phone.replaceAll("[^+\\d]", "");

                String formatted = digits;
                if (digits.length() == 11 && (digits.startsWith("8") || digits.startsWith("7"))) {
                    formatted = "+7" + digits.substring(1);
                } else if (digits.length() == 10) {
                    formatted = "+7" + digits;
                } else if (normalized.startsWith("+7")) {
                    formatted = normalized;
                } else {
                    continue;
                }

                if (
                        formatted.matches("\\+7\\d{10}") &&
                                seenPhones.add(formatted) &&
                                !repo.existsBySourceUrlAndPhone(url, formatted)
                ) {
                    ContactInfo info = new ContactInfo();
                    info.setSourceUrl(url);
                    info.setPhone(formatted);
                    repo.save(info);
                    System.out.println("📞 Найден телефон: " + formatted);
                }
            }

            Set<String> seenAddresses = new HashSet<>();

            Matcher mAddr = ADDRESS.matcher(text);
            while (mAddr.find()) {
                String addr = mAddr.group().trim();
                if (!addr.isBlank() &&
                        seenAddresses.add(addr) &&
                        !repo.existsBySourceUrlAndAddress(url, addr)) {

                    ContactInfo info = new ContactInfo();
                    info.setSourceUrl(url);
                    info.setAddress(addr);
                    repo.save(info);
                    System.out.println("📍Найден адресс: " + addr);
                }
            }
            Elements links = doc.select("a[href]");
            long added = links.stream()
                    .map(e -> e.absUrl("href"))
                    .filter(h -> h.startsWith("http"))
                    .filter(h -> !h.contains("#"))
                    .filter(h -> visited.add(h))
                    .peek(frontier::add)
                    .count();

            System.out.println("🔗 Links added to frontier: " + added);

        } catch (Exception e) {
            System.out.println("❌ Error processing " + url + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
        }
    }

    public PageResult getResults(int page, int size, String sortBy) {
        var all = repo.findAll();
        var stream = all.parallelStream();
        if ("email".equalsIgnoreCase(sortBy)) stream = stream.sorted((a,b)-> nullSafe(a.getEmail()).compareTo(nullSafe(b.getEmail())));
        if ("phone".equalsIgnoreCase(sortBy)) stream = stream.sorted((a,b)-> nullSafe(a.getPhone()).compareTo(nullSafe(b.getPhone())));
        var list = stream.skip((long) page * size).limit(size).toList();
        return new PageResult(list, all.size());
    }

    private String nullSafe(String s){ return s==null?"":s; }

    public record PageResult(java.util.List<ContactInfo> items, int total) {}
}