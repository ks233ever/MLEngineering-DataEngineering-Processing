from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    """
    This Operator loads data from Redshift staging tables to a Fact table
    """
      

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql = load_sql
     

    def execute(self, context):
        self.log.info('Grabbing credentials')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Inserting data to Fact table in Redshift")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.load_sql
        )
        redshift.run(formatted_sql)
        
        
        
        self.log.info("Inserting data into Redshift")
        formatted_sql = LoadFactOperator.copy_sql.format(
            self.sql_query,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter
        )
        redshift.run(formatted_sql)
        
        
        
