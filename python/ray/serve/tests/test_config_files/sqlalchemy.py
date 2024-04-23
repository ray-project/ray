from ray import serve


@serve.deployment
class TestDeployment:
    def __init__(self):
        import pymysql
        from sqlalchemy import create_engine

        pymysql.install_as_MySQLdb()

        create_engine("mysql://some_wrong_url:3306").connect()


app = TestDeployment.bind()
