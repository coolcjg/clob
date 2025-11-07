package com.cjg.clob.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Arrays;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class SimpleChunkJobConfig {

    private final int CHUNK_SIZE = 5;

    @Bean
    public Job simpleChunkJob(JobRepository jobRepository, Step simpleChunkStep) {
        return new JobBuilder("simpleChunkJob", jobRepository)
                .start(simpleChunkStep)
                .build();
    }

    @Bean
    public Step simpleChunkStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                ItemReader<Integer> simpleReader,
                                ItemProcessor<Integer, String> simpleProcessor,
                                ItemWriter<String> simpleWriter){

        return new StepBuilder("simpleChunkStep", jobRepository)
                .<Integer, String>chunk(CHUNK_SIZE, transactionManager)
                .reader(simpleReader)
                .processor(simpleProcessor)
                .writer(simpleWriter)
                .build();
    }

    @Bean
    public ItemReader<Integer> simpleReader(){
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        log.info(">>>>> Reader가 데이터를 읽습니다: {}", numbers);
        return new ListItemReader<>(numbers);
    }

    @Bean
    public ItemProcessor<Integer, String> simpleProcessor(){
        return item -> {
            if(item % 2 == 0){
                String processedItem = "Processed Item : " + (item*10);
                log.info(">>>>> Processor가 데이터를 가공합니다 : {} -> {}", item, processedItem);
                return processedItem;
            }

            log.info(" >>>> Processor가 데이터를 필터링합니다 (홀수) : {}", item);
            return null;
        };
    }

    @Bean
    public ItemWriter<String> simpleWriter(){
        return chunk -> {
            log.info(" >>>> Writer가 Chunk단위 데이터를 씁니다: {}",  chunk.getItems());
        };
    }






}
