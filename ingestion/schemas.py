from datetime import datetime
import uuid
from pydantic import BaseModel, Field, field_validator
import pymongo


class WeatherEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    city: str
    latitude: float
    longitude: float
    temperature_2m: float = Field(ge=-10, le=60)
    relative_humidity_2m: float = Field(ge=0, le=100)
    wind_speed_10m: float = Field(ge=0)
    precipitation: float = Field(default=0.0, ge=0)
    timestamp: datetime
    # Below line of code will be used if custome vaidations required:
    # @field_validator("temperature_2m")
    # @classmethods
    # def temperature_validator(cls, temp):
    #     if not -10<=temp<=60:
    #         raise ValueError("Temperature is not in given range",temp)
    #     return temp
    
    # @field_validator("relative_humidity_2m")
    # @classmethod
    # def humidity_validator(cls, humi):
    #     if not (0 <= humi <= 100):
    #         raise ValueError(f"{humi} is out of range")
    #     return humi
    
    # @field_validator("wind_speed_10m")
    # @classmethod
    # def wind_validator(cls, speed):
    #     if speed < 0:
    #         raise ValueError(f"Speed {speed} is out of range")
    #     return speed
    
    # @field_validator("precipitation")
    # @classmethod
    # def precipitate_validator(cls,p):
    #     if p <0:
    #         raise ValueError(f"Precipitation {p} is out of range")
    #     return p
    

    def to_mongo_dict(self):
        return self.model_dump(mode="json")
        
    

# event = WeatherEvent(city="Bengaluru",
#     latitude=12.9716,
#     longitude=77.5946,
#     temperature_2m=28.4,
#     relative_humidity_2m=23.0,
#     wind_speed_10m=12.3
#     )
   




