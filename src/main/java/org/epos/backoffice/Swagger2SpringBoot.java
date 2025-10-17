package org.epos.backoffice;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.epos.backoffice.configuration.LocalDateConverter;
import org.epos.backoffice.configuration.LocalDateTimeConverter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication
public class Swagger2SpringBoot implements CommandLineRunner {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    public static void main(String[] args) {
        SpringApplication.run(Swagger2SpringBoot.class, args);
    }

    @Override
    public void run(String... args) {
        if (args.length > 0 && "exitcode".equals(args[0])) {
            throw new ExitException();
        }
    }

    // Se vuoi puoi anche spostare questa classe in un file separato, più pulito
    @Configuration
    static class CustomDateConfig implements WebMvcConfigurer {

        @Override
        public void addFormatters(FormatterRegistry registry) {
            registry.addConverter(new LocalDateConverter("yyyy-MM-dd"));
            registry.addConverter(new LocalDateTimeConverter("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
        }
    }

    static class ExitException extends RuntimeException implements ExitCodeGenerator {
        private static final long serialVersionUID = 1L;

        @Override
        public int getExitCode() {
            return 10;
        }
    }
}
