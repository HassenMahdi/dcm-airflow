import json
import pickle
from json import JSONDecodeError
from typing import Any, Iterable, Optional, Union

from sqlalchemy import DateTime, and_
from sqlalchemy.orm import reconstructor

from api.db.airflow import db
from api.utils.utils import is_container

MAX_XCOM_SIZE = 49344
XCOM_RETURN_KEY = 'return_value'

ENABLE_PICKELING = True


class BaseXCom(db.Model):
    """Base class for XCom objects."""

    __tablename__ = "xcom"

    key = db.Column(db.String, primary_key=True)
    value = db.Column(db.LargeBinary)
    timestamp = db.Column(db.DateTime, nullable=False)
    execution_date = db.Column(db.DateTime, primary_key=True)

    # source information
    task_id = db.Column(db.String, primary_key=True)
    dag_id = db.Column(db.String, primary_key=True)

    @reconstructor
    def init_on_load(self):
        """
        Called by the ORM after the instance has been loaded from the DB or otherwise reconstituted
        i.e automatically deserialize Xcom value when loading from DB.
        """
        try:
            self.value = self.orm_deserialize_value()
        except (UnicodeEncodeError, ValueError):
            # For backward-compatibility.
            # Preventing errors in webserver
            # due to XComs mixed with pickled and unpickled.
            self.value = pickle.loads(self.value)

    def __repr__(self):
        return f'<XCom "{self.key}" ({self.task_id} @ {self.execution_date})>'

    @classmethod
    def set(cls, key, value, execution_date, task_id, dag_id, session=None):
        """
        Store an XCom value.
        :return: None
        """
        session.expunge_all()

        value = XCom.serialize_value(value)

        # remove any duplicate XComs
        session.query(cls).filter(
            cls.key == key, cls.execution_date == execution_date, cls.task_id == task_id, cls.dag_id == dag_id
        ).delete()

        session.commit()

        # insert new XCom
        session.add(XCom(key=key, value=value, execution_date=execution_date, task_id=task_id, dag_id=dag_id))

        session.commit()

    @classmethod
    def get_one(
        cls,
        execution_date: DateTime,
        key: Optional[str] = None,
        task_id: Optional[Union[str, Iterable[str]]] = None,
        dag_id: Optional[Union[str, Iterable[str]]] = None,
        include_prior_dates: bool = False,
    ) -> Optional[Any]:
        result = cls.get_many(
            execution_date=execution_date,
            key=key,
            task_ids=task_id,
            dag_ids=dag_id,
            include_prior_dates=include_prior_dates,
        ).first()
        if result:
            return result.value
        return None

    @classmethod
    def get_many(
        cls,
        execution_date: DateTime,
        key: Optional[str] = None,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        dag_ids: Optional[Union[str, Iterable[str]]] = None,
        include_prior_dates: bool = False,
        limit: Optional[int] = None,
    ):
        filters = []

        if key:
            filters.append(cls.key == key)

        if task_ids:
            if is_container(task_ids):
                filters.append(cls.task_id.in_(task_ids))
            else:
                filters.append(cls.task_id == task_ids)

        if dag_ids:
            if is_container(dag_ids):
                filters.append(cls.dag_id.in_(dag_ids))
            else:
                filters.append(cls.dag_id == dag_ids)

        if include_prior_dates:
            filters.append(cls.execution_date <= execution_date)
        else:
            filters.append(cls.execution_date == execution_date)

        query = (
            cls.query
            .filter(and_(*filters))
            .order_by(cls.execution_date.desc(), cls.timestamp.desc())
        )

        if limit:
            return query.limit(limit)
        else:
            return query

    @classmethod
    def delete(cls, xcoms, session=None):
        """Delete Xcom"""
        if isinstance(xcoms, XCom):
            xcoms = [xcoms]
        for xcom in xcoms:
            if not isinstance(xcom, XCom):
                raise TypeError(f'Expected XCom; received {xcom.__class__.__name__}')
            session.delete(xcom)
        session.commit()

    @staticmethod
    def serialize_value(value: Any):
        """Serialize Xcom value to str or pickled object"""
        enable_pickling = ENABLE_PICKELING
        if enable_pickling:
            return pickle.dumps(value)
        try:
            return json.dumps(value).encode('UTF-8')
        except (ValueError, TypeError):
            raise Exception(
                "Could not serialize the XCom value into JSON. "
                "If you are using pickles instead of JSON "
                "for XCom, then you need to enable pickle "
                "support for XCom in your airflow config."
            )

    @staticmethod
    def deserialize_value(result: "XCom") -> Any:
        """Deserialize XCom value from str or pickle object"""
        enable_pickling = ENABLE_PICKELING
        if enable_pickling:
            try:
                return pickle.loads(result.value)
            except pickle.UnpicklingError:
                return json.loads(result.value.decode('UTF-8'))
        try:
            return json.loads(result.value.decode('UTF-8'))
        except JSONDecodeError:
            raise Exception(
                "Could not deserialize the XCom value from JSON. "
                "If you are using pickles instead of JSON "
                "for XCom, then you need to enable pickle "
                "support for XCom in your airflow config."
            )


    def orm_deserialize_value(self) -> Any:
        return BaseXCom.deserialize_value(self)


XCom = BaseXCom