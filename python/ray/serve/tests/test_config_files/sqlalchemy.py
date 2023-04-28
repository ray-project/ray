from ray import serve

# TestDeployment should be deployed with a runtime env that installs pip dependencies
# PyMySQL and sqlalchemy.
@serve.deployment
class TestDeployment:
    def __init__(self):
        from sqlalchemy import create_engine
        import pymysql

        pymysql.install_as_MySQLdb()

        create_engine("mysql://some_wrong_url:3306").connect()


app = TestDeployment.bind()
