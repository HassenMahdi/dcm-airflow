from api.db.airflow import db


class DagModel(db.Model):
    """Table containing DAG properties"""

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information
    """
    dag_id = db.Column(db.String, primary_key=True)
    root_dag_id = db.Column(db.String)
    # A DAG can be paused from the UI / DB
    # Set this default value of is_paused based on a configuration value!
    is_paused = db.Column(db.Boolean)
    # Whether that DAG was seen on the last DagBag load
    is_active = db.Column(db.Boolean, default=False)
    # Last time the scheduler started
    last_scheduler_run = db.Column(db.DateTime)
    # Last time this DAG was pickled
    last_pickled = db.Column(db.DateTime)
    # Time when the DAG last received a refresh signal
    # (e.g. the DAG's "refresh" button was clicked in the web UI)
    last_expired = db.Column(db.DateTime)
    # Whether (one  of) the scheduler is scheduling this DAG at the moment
    scheduler_lock = db.Column(db.Boolean)
    # Foreign key to the latest pickle_id
    schedule_interval = db.Column(db.Interval)

    def __repr__(self):
        return f"<DAG: {self.dag_id}>"

    @staticmethod
    def get_dagmodel(dag_id, session=None):
        return DagModel.query.filter(DagModel.dag_id == dag_id).first()

    @staticmethod
    def change_pause_state(dag_id, paused, session=None):
        session = session or db.session
        try:
            qry = session.query(DagModel).filter(DagModel.dag_id == dag_id)
            d = qry.first()
            d.is_paused = paused
            session.commit()
        except:
            session.rollback()
        finally:
            session.close()