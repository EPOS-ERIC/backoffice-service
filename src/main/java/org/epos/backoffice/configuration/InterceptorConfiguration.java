package org.epos.backoffice.configuration;

import org.epos.backoffice.api.controller.interceptor.LogUserInInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

@Configuration
public class InterceptorConfiguration implements WebMvcConfigurer {
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new LogUserInInterceptor()).addPathPatterns(
                List.of(
                        "/user/**",
                        "/group/**",
                        "/address/**",
                        "/category/**",
                        "/categoryscheme/**",
                        "/contactpoint/**",
                        "/dataproduct/**",
                        "/distribution/**",
                        "/documentation/**",
                        "/element/**",
                        "/identifier/**",
                        "/legalname/**",
                        "/location/**",
                        "/mapping/**",
                        "/operation/**",
                        "/organization/**",
                        "/parameter/**",
                        "/periodoftime/**",
                        "/person/**",
                        "/webservice/**",
                        "/populate-ttl/**"
                )
        );
    }

}
