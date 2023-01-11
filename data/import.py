import matplotlib.pyplot as plt
import mpld3
from sqlalchemy import Column, Integer, DateTime, Numeric, create_engine
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.exc import IntegrityError
import csv
import click
from decimal import Decimal
from datetime import datetime


Base = declarative_base()


class Data(Base):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime(), unique=True)
    pm2_5 = Column(Numeric())
    pm10 = Column(Numeric())
    co2 = Column(Numeric())
    hcho = Column(Numeric())
    temperature = Column(Numeric())
    humidity = Column(Numeric())

@click.group()
def cli():
    pass


def connect():
    engine = create_engine("sqlite:///data.sqlite3")
    Base.metadata.create_all(engine)
    return engine


@cli.command('graph')
def graph():
    engine = connect()
    plt.plot([3,1,4,1,5], 'ks-', mec='w', mew=5, ms=20)
    mpld3.show()

@cli.command('import')
@click.argument('file', type=click.File(), required=True)
def import_(file):
    engine = connect()
    reader = csv.DictReader(file)
    with Session(engine) as session:
        for r in reader:
            def f(name, constructor=None):
                C = (Decimal if constructor is None else constructor)
                return C(r[name])
            temp = f('TEMPERATURE')
            if r['TEMPUNIT'] == 'F':
                temp = (temp - 30) / 2
            d = Data(
                date=f('DATE', lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S')),
                pm2_5=f('PM2.5(ug/m3)'),
                pm10=f('PM10(ug/m3)'),
                co2=f('CO2(ppm)'),
                hcho=f('HCHO(mg/m3)'),
                temperature=temp,
                humidity=f('HUMIDITY(%)'),
            )
            session.add(d)
            try:
                session.commit()
            except IntegrityError as ex:
                session.rollback()
                click.echo(f'Could not add record with timestamp "{r["DATE"]}".  Skipping.', err=True)
                continue


if __name__ == '__main__':
    cli()
