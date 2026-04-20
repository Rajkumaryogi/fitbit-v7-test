from . import db
from datetime import datetime, timezone

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fitbit_user_id = db.Column(db.String(100), unique=True, nullable=False)
    access_token = db.Column(db.Text, nullable=False)
    refresh_token = db.Column(db.Text, nullable=False)
    last_sync = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    heart_rate = db.relationship('HeartRate', backref='user', lazy=True)
    sleep = db.relationship('Sleep', backref='user', lazy=True)
    steps = db.relationship('Steps', backref='user', lazy=True)
    weight = db.relationship('Weight', backref='user', lazy=True)
    activities = db.relationship('Activity', backref='user', lazy=True)
    nutrition = db.relationship('Nutrition', backref='user', lazy=True)
    hydration = db.relationship('Hydration', backref='user', lazy=True)
    blood_pressure = db.relationship('BloodPressure', backref='user', lazy=True)
    body_fat = db.relationship('BodyFat', backref='user', lazy=True)
    oxygen_saturation = db.relationship('OxygenSaturation', backref='user', lazy=True)
    respiratory_rate = db.relationship('RespiratoryRate', backref='user', lazy=True)
    temperature = db.relationship('Temperature', backref='user', lazy=True)

class HeartRate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    resting_heart_rate = db.Column(db.Integer)

class Sleep(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    total_minutes_asleep = db.Column(db.Integer)
    time_in_bed = db.Column(db.Integer)
    efficiency = db.Column(db.Float)

class Steps(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    steps = db.Column(db.Integer)

class Weight(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    weight = db.Column(db.Float)

class Activity(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    activity_name = db.Column(db.String(128))
    duration_minutes = db.Column(db.Integer)
    calories_burned = db.Column(db.Integer)

class Nutrition(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    calories_consumed = db.Column(db.Integer)
    protein_grams = db.Column(db.Float)
    carbs_grams = db.Column(db.Float)
    fats_grams = db.Column(db.Float)

class Hydration(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    water_intake_ounces = db.Column(db.Float)

class BloodPressure(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    systolic = db.Column(db.Integer)
    diastolic = db.Column(db.Integer)

class BodyFat(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    body_fat_percentage = db.Column(db.Float)

class OxygenSaturation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    oxygen_saturation_percentage = db.Column(db.Float)

class RespiratoryRate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    respiratory_rate_bpm = db.Column(db.Float)

class Temperature(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    date = db.Column(db.String(32))
    body_temperature_f = db.Column(db.Float)
