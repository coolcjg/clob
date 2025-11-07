package com.cjg.clob.config;

import com.cjg.clob.data.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Slf4j
@Configuration
public class ClobJobConfig {

    private final int CHUNK_SIZE = 5;

    private final DataSource mainDataSource;
    private final DataSource secondDataSource;

    public ClobJobConfig(
            @Qualifier("mainDataSource")DataSource mainDataSource
            , @Qualifier("secondDataSource") DataSource secondDataSource){
        this.mainDataSource = mainDataSource;
        this.secondDataSource = secondDataSource;
    }

    @Bean
    public Job clobJob(JobRepository jobRepository, Step simpleChunkStep) {
        return new JobBuilder("clobJob-" + System.currentTimeMillis(), jobRepository)
                .start(simpleChunkStep)
                .build();
    }

    @Bean
    public Step simpleChunkStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                ItemReader<Customer> customerCursorItemReader,
                                ItemProcessor<Customer, Customer> customerProcessor,
                                ItemWriter<Customer> customerWriter){

        return new StepBuilder("clobStep", jobRepository)
                .<Customer, Customer>chunk(CHUNK_SIZE, transactionManager)
                .reader(customerCursorItemReader)
                .processor(customerProcessor)
                .writer(customerWriter)
                .build();
    }

    @Bean
    public JdbcCursorItemReader<Customer> customerCursorItemReader(){
        String sql = """
                SELECT
                    ID,
                    NAME,
                    MEMO
                FROM
                    CUSTOMER
                ORDER BY 
                    ID ASC
                """;

        return new JdbcCursorItemReaderBuilder<Customer>()
                .name("customerCursorItemReader")
                .dataSource(mainDataSource)
                .sql(sql)
                .rowMapper(new DataClassRowMapper<>(Customer.class))
                .fetchSize(100)
                .build();
    }

    @Bean
    public ItemProcessor<Customer, Customer> customerProcessor(){
        return customer -> {
            log.info(">>>> Processing Customer : {}" , customer);
            return customer;
        };
    }

    @Bean
    public ItemWriter<Customer> customerWriter(){

        String sql = """
            INSERT INTO 
                CUSTOMER (ID, NAME, MEMO) 
            VALUES 
                (:id, :name, :memo)
            """;

        return new JdbcBatchItemWriterBuilder<Customer>()
                // 💡 @Qualifier로 주입받은 secondDataSource를 명시적으로 사용
                .dataSource(secondDataSource)

                // 💡 Item 객체의 필드를 SQL의 명명된 파라미터(:id, :name)에 매핑
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())

                // 삽입할 SQL 쿼리 설정
                .sql(sql)

                // 모든 레코드가 성공적으로 처리되었는지 검증 (선택적)
                //.assertUpdates(true)
                .build();
    }

//    @Bean
//    public ItemReader<Integer> simpleReader(){
//        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
//        log.info(">>>>> Reader가 데이터를 읽습니다: {}", numbers);
//        return new ListItemReader<>(numbers);
//    }

//    @Bean
//    public ItemProcessor<Integer, String> simpleProcessor(){
//        return item -> {
//            if(item % 2 == 0){
//                String processedItem = "Processed Item : " + (item*10);
//                log.info(">>>>> Processor가 데이터를 가공합니다 : {} -> {}", item, processedItem);
//                return processedItem;
//            }
//
//            log.info(" >>>> Processor가 데이터를 필터링합니다 (홀수) : {}", item);
//            return null;
//        };
//    }

//    @Bean
//    public ItemWriter<String> simpleWriter(){
//        return chunk -> {
//            log.info(" >>>> Writer가 Chunk단위 데이터를 씁니다: {}",  chunk.getItems());
//        };
//    }






}
