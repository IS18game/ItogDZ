package org.example.config;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
public class AppConfig {

    @Bean(destroyMethod = "shutdown")
    public ExecutorService crawlerExecutor() {
        return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }

    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService scheduledExecutor() {
        return Executors.newScheduledThreadPool(1);
    }

    @Bean
    public RestTemplate restTemplate() {
        CloseableHttpClient client = HttpClients.custom()
                .setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                .setDefaultHeaders(List.of(
                        new BasicHeader(HttpHeaders.ACCEPT, "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
                        new BasicHeader(HttpHeaders.ACCEPT_LANGUAGE, "en-US,en;q=0.5")
                ))
                .build();

        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(client);
        return new RestTemplate(factory);
    }
}