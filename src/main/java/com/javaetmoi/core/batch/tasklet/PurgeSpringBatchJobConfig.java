package com.javaetmoi.core.batch.tasklet;

import java.sql.Timestamp;
import java.time.LocalDate;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.StringUtils;

@Configuration
public class PurgeSpringBatchJobConfig {

    private static final Integer DEFAULT_DAYS_TO_RETAIN = 7;

    private static final String DEFAULT_TABLE_PREFIX = AbstractJdbcBatchMetadataDao.DEFAULT_TABLE_PREFIX;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    @Qualifier(value = "batchDataSource")
    private DataSource batchDataSource;

    private String tablePrefix = DEFAULT_TABLE_PREFIX;

    private Integer daysToRetain = DEFAULT_DAYS_TO_RETAIN;

    private static final String SELECT_PURGABLE_STEP_EXECUTIONS = "SELECT STEP_EXECUTION_ID FROM %PREFIX%STEP_EXECUTION WHERE JOB_EXECUTION_ID IN (SELECT JOB_EXECUTION_ID FROM  %PREFIX%JOB_EXECUTION where CREATE_TIME < ?)";

    private static final String DELETE_STEP_EXECUTION_CONTEXT = "DELETE FROM %PREFIX%STEP_EXECUTION_CONTEXT WHERE STEP_EXECUTION_ID = :stepExecutionId";

    private static final String DELETE_STEP_EXECUTION = "DELETE FROM %PREFIX%STEP_EXECUTION WHERE STEP_EXECUTION_ID = :stepExecutionId";

    private static final String SELECT_PURGABLE_JOB_EXECUTIONS = "SELECT JOB_EXECUTION_ID FROM %PREFIX%JOB_EXECUTION where CREATE_TIME < ?";

    private static final String DELETE_JOB_EXECUTION_CONTEXT = "DELETE FROM %PREFIX%JOB_EXECUTION_CONTEXT WHERE JOB_EXECUTION_ID = :jobExecutionId";

    private static final String DELETE_JOB_EXECUTION_PARAMS = "DELETE FROM %PREFIX%JOB_EXECUTION_PARAMS WHERE JOB_EXECUTION_ID = :jobExecutionId";

    private static final String DELETE_JOB_EXECUTIONS = "DELETE FROM %PREFIX%JOB_EXECUTION WHERE JOB_EXECUTION_ID = :jobExecutionId";

    private static final String SELECT_PURGABLE_JOB_INSTANCES = "SELECT JOB_INSTANCE_ID FROM %PREFIX%JOB_INSTANCE WHERE JOB_INSTANCE_ID NOT IN (SELECT JOB_INSTANCE_ID FROM %PREFIX%JOB_EXECUTION)";

    private static final String DELETE_JOB_INSTANCES = "DELETE FROM %PREFIX%JOB_INSTANCE WHERE JOB_INSTANCE_ID = :jobInstanceId";

    @Bean
    @StepScope
    public JdbcCursorItemReader<String> purgableStepExecutionReader() {
        return new JdbcCursorItemReaderBuilder<String>()
                .name("purgableStepExecutionReader")
                .dataSource(batchDataSource)
                .sql(getQuery(SELECT_PURGABLE_STEP_EXECUTIONS))
                .queryArguments(Timestamp.valueOf(LocalDate.now().minusDays(daysToRetain).atStartOfDay()))
                .rowMapper((rs, i) -> rs.getString("STEP_EXECUTION_ID"))
                .saveState(false)
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<String> stepExecutionContextDeleter() {
        return new JdbcBatchItemWriterBuilder<String>()
                .dataSource(batchDataSource)
                .sql(getQuery(DELETE_STEP_EXECUTION_CONTEXT))
                .itemSqlParameterSourceProvider(
                        (stepExecutionId) -> new MapSqlParameterSource("stepExecutionId", stepExecutionId))
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<String> stepExecutionDeleter() {
        return new JdbcBatchItemWriterBuilder<String>()
                .dataSource(batchDataSource)
                .sql(getQuery(DELETE_STEP_EXECUTION))
                .itemSqlParameterSourceProvider(
                        (stepExecutionId) -> new MapSqlParameterSource("stepExecutionId", stepExecutionId))
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<String> stepExecutionsDeleter() {
        return new CompositeItemWriterBuilder<String>()
                .delegates(stepExecutionContextDeleter(),
                        stepExecutionDeleter())
                .build();
    }

    @Bean
    public Step deleteStepExecutionContexts() {
        return stepBuilderFactory.get("deleteStepExecutions").<String, String>chunk(100)
                .reader(purgableStepExecutionReader())
                .writer(stepExecutionsDeleter())
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<String> purgableJobExecutionIdReader() {
        return new JdbcCursorItemReaderBuilder<String>()
                .name("purgableJobExecutionIdReader")
                .dataSource(batchDataSource)
                .sql(getQuery(SELECT_PURGABLE_JOB_EXECUTIONS))
                .queryArguments(Timestamp.valueOf(LocalDate.now().minusDays(daysToRetain).atStartOfDay()))
                .rowMapper((rs, i) -> rs.getString("JOB_EXECUTION_ID"))
                .saveState(false)
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<String> jobExecutionContextDeleter() {
        return new JdbcBatchItemWriterBuilder<String>()
                .dataSource(batchDataSource)
                .sql(getQuery(DELETE_JOB_EXECUTION_CONTEXT))
                .itemSqlParameterSourceProvider(
                        (jobExecutionId) -> new MapSqlParameterSource("jobExecutionId", jobExecutionId))
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<String> jobExecutionParamsDeleter() {
        return new JdbcBatchItemWriterBuilder<String>()
                .dataSource(batchDataSource)
                .sql(getQuery(DELETE_JOB_EXECUTION_PARAMS))
                .itemSqlParameterSourceProvider(
                        (jobExecutionId) -> new MapSqlParameterSource("jobExecutionId", jobExecutionId))
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<String> jobExecutionDeleter() {
        return new JdbcBatchItemWriterBuilder<String>()
                .dataSource(batchDataSource)
                .sql(getQuery(DELETE_JOB_EXECUTIONS))
                .itemSqlParameterSourceProvider(
                        (jobExecutionId) -> new MapSqlParameterSource("jobExecutionId", jobExecutionId))
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<String> jobExecutionsDeleter() {
        return new CompositeItemWriterBuilder<String>()
                .delegates(jobExecutionContextDeleter(),
                        jobExecutionParamsDeleter(),
                        jobExecutionDeleter())
                .build();
    }

    @Bean
    public Step deleteJobExecutions() {
        return stepBuilderFactory.get("deleteJobExecutions").<String, String>chunk(100)
                .reader(purgableJobExecutionIdReader())
                .writer(jobExecutionsDeleter())
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<String> purgableJobInstanceIdReader() {
        return new JdbcCursorItemReaderBuilder<String>()
                .name("purgableJobInstanceIdReader")
                .dataSource(batchDataSource)
                .sql(getQuery(SELECT_PURGABLE_JOB_INSTANCES))
                .rowMapper((rs, i) -> rs.getString("JOB_INSTANCE_ID"))
                .saveState(false)
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<String> jobInstanceDeleter() {
        return new JdbcBatchItemWriterBuilder<String>()
                .dataSource(batchDataSource)
                .sql(getQuery(DELETE_JOB_INSTANCES))
                .itemSqlParameterSourceProvider(
                        (jobInstanceId) -> new MapSqlParameterSource("jobInstanceId", jobInstanceId))
                .build();
    }

    @Bean
    public Step deleteJobInstances() {
        return stepBuilderFactory.get("deleteJobInstances").<String, String>chunk(100)
                .reader(purgableJobInstanceIdReader())
                .writer(jobInstanceDeleter())
                .build();
    }

    @Bean
    public Job purgeSpringBatchJob() {
        return jobBuilderFactory
                .get("purgeSpringBatch")
                .incrementer(new RunIdIncrementer())
                .start(deleteStepExecutionContexts())
                .next(deleteJobExecutions())
                .next(deleteJobInstances())
                .build();
    }

    protected String getQuery(String base) {
        return StringUtils.replace(base, "%PREFIX%", tablePrefix);
    }

}
