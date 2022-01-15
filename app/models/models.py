"""[summary]

Returns:
    [type]: [description]
"""
import os

from app.models.base import Base
from app.utils import db_session, get_db_type
from sqlalchemy.orm import validates
from sqlalchemy.sql.sqltypes import Boolean, Date, Integer, String
from sqlalchemy.sql.schema import CheckConstraint, Column

# https://docs.sqlalchemy.org/en/14/orm/self_referential.html


class NIS_Code(Base):
    __tablename__ = "dim_nis_codes"

    nis = Column(String(5), primary_key=True)
    # parent_nis = Column(String(5), ForeignKey('dim_nis_codes.nis'), nullable=True)
    parent_nis = Column(String(5), nullable=True)
    level = Column(Integer, nullable=False)
    text_nl = Column(String(255))
    text_fr = Column(String(255))
    text_de = Column(String(255))
    valid_from = Column(Date, primary_key=True)
    valid_till = Column(Date, nullable=False)

    # children = relationship(
    #     "NIS_Code",
    #     lazy='select',
    #     backref=backref('parent', remote_side=[nis])
    # )

    __table_args__ = tuple(
        (CheckConstraint("LEN(nis)=5"), CheckConstraint("LEN(parent_nis)=5"))
        if (
            get_db_type()
            == "mssql"
            # os.environ.get('DATABASE_URL')
        )
        else (
            CheckConstraint("length(nis)==5"),
            CheckConstraint("length(parent_nis)==5"),
        ),
    )

    def __repr__(self):
        return """
            <NIS_Code(level='%s', nis='%s', name='%s', parent='%s')>
            """ % (
            self.level,
            self.nis,
            self.text_nl,
            self.parent_nis,
        )

    @validates("nis")
    def validate_nis(self, key, nis) -> str:
        if len(nis) != 5:
            raise ValueError("'nis' should be 5 characters long..")
        return nis

    @validates("parent_nis")
    def validate_parent_nis(self, key, parent_nis) -> str:
        if not parent_nis:
            return parent_nis
        if len(parent_nis) != 5:
            raise ValueError("'parent_nis' should be 5 characters long..")
        return parent_nis

    @classmethod
    def get_all(cls):
        with db_session(echo=False) as session:
            local_nis_codes_all = session.query(NIS_Code).all()
            session.close()
            return local_nis_codes_all


class CovidVaccinationByCategory(Base):
    __tablename__ = "fact_covid_vaccinations_by_category"

    date = Column(Date, primary_key=True, nullable=False)
    region = Column(String(255), primary_key=True, nullable=False)
    agegroup = Column(String(32), primary_key=True, nullable=False)
    sex = Column(String(5), primary_key=True, nullable=False)
    brand = Column(String(255), primary_key=True, nullable=False)
    dose = Column(String(5), primary_key=True, nullable=False)
    count = Column(Integer, nullable=False)

    def __repr__(self):
        return """
            <CovidVaccinationByCategory(date='%s', region='%s', agegroup='%s', count='%s')>
            """ % (
            self.date,
            self.region,
            self.agegroup,
            self.count,
        )


class CovidMortalityByCategory(Base):
    __tablename__ = "fact_covid_mortality_by_category"

    date = Column(Date, primary_key=True)
    region = Column(String(255), primary_key=True)
    agegroup = Column(String(32), primary_key=True)
    sex = Column(String(5), primary_key=True)
    deaths = Column(Integer, nullable=False)

    def __repr__(self):
        return """
            <CovidMortalityByCategory(date='%s', region='%s', agegroup='%s', agegroup='%s', deaths='%s')>
            """ % (
            self.date,
            self.region,
            self.agegroup,
            self.sex,
            self.deaths,
        )


class CovidConfirmedCasesByCategory(Base):
    __tablename__ = "fact_covid_confirmed_cases_by_category"

    date = Column(Date, primary_key=True)
    province = Column(String(255), primary_key=True)
    region = Column(String(255), primary_key=True)
    agegroup = Column(String(32), primary_key=True)
    sex = Column(String(5), primary_key=True)
    cases = Column(Integer, nullable=False)

    def __repr__(self):
        return """
            <CovidConfirmedCasesByCategory(date='%s', region='%s', agegroup='%s', sex='%s', cases='%s')>
            """ % (
            self.date,
            self.region,
            self.agegroup,
            self.sex,
            self.cases,
        )


class DemographicsByNISCodeAndCategory(Base):
    __tablename__ = "fact_demographics_by_nis_code_and_category"

    year = Column(Integer, primary_key=True)
    nis = Column(String(5), primary_key=True)
    sex = Column(String(5), primary_key=True)
    nationality_code = Column(String(5), primary_key=True)
    nationality_text_nl = Column(String(255))
    nationality_text_fr = Column(String(255))
    marital_status_code = Column(String(5), primary_key=True)
    marital_status_text_nl = Column(String(255))
    marital_status_text_fr = Column(String(255))
    age = Column(Integer, primary_key=True)
    population = Column(Integer)

    __table_args__ = tuple(
        (CheckConstraint("LEN(nis)=5"))
        if (get_db_type() == "mssql")
        else (CheckConstraint("length(nis)==5")),
    )

    def __repr__(self):
        return """
            <DemographicsByNISCodeAndCategory(year='%s', nis='%s', sex='%s', nationality_code='%s', age='%s', population='%s')>
            """ % (
            self.year,
            self.nis,
            self.nationality_code,
            self.sex,
            self.age,
            self.population,
        )

    @validates("nis")
    def validate_nis(self, key, nis) -> str:
        if len(nis) != 5:
            raise ValueError("'nis' should be 5 characters long..")
        return nis


class NumberOfDeathsByDistrictNISCode(Base):
    __tablename__ = "fact_number_of_deaths_by_district_nis_code"

    date = Column(Date, primary_key=True)
    nis_district = Column(String(5), primary_key=True)
    sex = Column(String(5), primary_key=True)
    agegroup = Column(String(32), primary_key=True)
    number_of_deaths = Column(Integer, nullable=False)

    __table_args__ = tuple(
        (CheckConstraint("LEN(nis_district)=5"))
        if (get_db_type() == "mssql")
        else (CheckConstraint("length(nis_district)==5")),
    )

    def __repr__(self):
        return """
            <NumberOfDeathsByDistrictNISCode(date='%s', nis_district='%s', sex='%s', agegroup='%s', number_of_deaths='%s')>
            """ % (
            self.date,
            self.nis_district,
            self.sex,
            self.agegroup,
            self.number_of_deaths,
        )

    @validates("nis_district")
    def validate_nis_district(self, key, nis_district) -> str:
        if len(nis_district) != 5:
            raise ValueError("'nis_district' should be 5 characters long..")
        return nis_district


class VaccinationsByNISCodeDailyUpdated(Base):
    __tablename__ = "fact_vaccinations_by_nis_code_daily_updated"

    nis_code = Column(String(5), primary_key=True)
    sex = Column(String(5), primary_key=True)
    agegroup = Column(String(32), primary_key=True)
    plus18 = Column(Boolean, primary_key=True)
    plus65 = Column(Boolean, primary_key=True)
    vaccinated_fully_total = Column(Integer, nullable=False)
    vaccinated_partly_total = Column(Integer, nullable=False)
    vaccinated_fully_astrazeneca = Column(Integer, nullable=False)
    vaccinated_partly_astrazeneca = Column(Integer, nullable=False)
    vaccinated_fully_pfizer = Column(Integer, nullable=False)
    vaccinated_partly_pfizer = Column(Integer, nullable=False)
    vaccinated_fully_moderna = Column(Integer, nullable=False)
    vaccinated_partly_moderna = Column(Integer, nullable=False)
    vaccinated_fully_johnsonandjohnson = Column(Integer, nullable=False)
    vaccinated_fully_other = Column(Integer, nullable=False)
    vaccinated_partly_other = Column(Integer, nullable=False)
    vaccinated_with_booster = Column(Integer, nullable=False, server_default="0")
    population_by_agecategory_and_municipality = Column(Integer, nullable=False)

    __table_args__ = tuple(
        (CheckConstraint("LEN(nis_code)=5"))
        if (get_db_type() == "mssql")
        else (CheckConstraint("length(nis_code)==5")),
    )

    def __repr__(self):
        return """
            <VaccinationsByNISCodeDailyUpdated(nis_code='%s', sex='%s', agegroup='%s', vaccinated_fully_total='%s')>
            """ % (
            self.nis_code,
            self.sex,
            self.agegroup,
            self.vaccinated_fully_total,
        )

    @validates("nis_code")
    def validate_nis_code(self, key, nis_code) -> str:
        if len(nis_code) != 5:
            raise ValueError("'nis_code' should be 5 characters long..")
        return nis_code


class VaccinationsByNISCodeAndWeek(Base):
    __tablename__ = "fact_vaccinations_by_nis_code_and_week"

    date = Column(Date, primary_key=True)
    year = Column(Integer, primary_key=True)
    week = Column(Integer, primary_key=True)
    nis_code = Column(String(5), primary_key=True)
    agegroup = Column(String(5), primary_key=True)
    dose = Column(String(5), primary_key=True)
    cumul_of_week = Column(Integer, nullable=False)

    __table_args__ = tuple(
        (CheckConstraint("LEN(nis_code)=5"))
        if (get_db_type() == "mssql")
        else (CheckConstraint("length(nis_code)==5")),
    )

    @validates("nis_code")
    def validate_nis_code(self, key, nis_code) -> str:
        if len(nis_code) != 5:
            raise ValueError("'nis_code' should be 5 characters long..")
        return nis_code

    def __repr__(self):
        return """
            <VaccinationsByNISCodeAndWeek(year='%s', week='%s', nis_code='%s', cumul_of_week='%s')>
            """ % (
            self.year,
            self.week,
            self.nis_code,
            self.cumul_of_week,
        )


class IncomeByNISCodeYearlyUpdated(Base):
    __tablename__ = "fact_income_by_nis_code_yearly_updated"
    
    year = Column(Integer(), primary_key=True)
    municipality_niscode = Column(Integer(), primary_key=True)
    nr_of_declarations = Column(Integer(), nullable=False)
    nr_zero_incomes = Column(Integer(), nullable=False)
    total_taxable_income = Column(Integer(), nullable=False)
    total_net_income = Column(Integer(), nullable=False)
    nr_total_income = Column(Integer(), nullable=False)
    total_net_professional_income = Column(Integer(), nullable=False)
    nr_net_professional_income = Column(Integer(), nullable=False)
    total_taxes = Column(Integer(), nullable=False)
    nr_taxes = Column(Integer(), nullable=False)
    nr_population = Column(Integer(), nullable=False)

    __table_args__ = tuple(
        (CheckConstraint("LEN(municipality_niscode)=5"))
        if (get_db_type() == "mssql")
        else (CheckConstraint("length(municipality_niscode)==5")),
    )

    @validates("municipality_niscode")
    def validate_municipality_niscode(self, key, municipality_niscode) -> str:
        if len(municipality_niscode) != 5:
            raise ValueError("'municipality_niscode' should be 5 characters long..")
        return municipality_niscode

    def __repr__(self):
        return """
            <VaccinationsByNISCodeAndWeek(year='%s', week='%s', nis_code='%s', cumul_of_week='%s')>
            """ % (
            self.year,
            self.week,
            self.nis_code,
            self.cumul_of_week,
        )