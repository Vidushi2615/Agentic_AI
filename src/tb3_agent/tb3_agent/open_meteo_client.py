import json
import urllib.parse
import urllib.request


BLOCKING_WEATHER_CODES = {
    51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 71, 73, 75, 77, 80, 81, 82, 85, 86
}


class OpenMeteoError(RuntimeError):
    pass


class OpenMeteoClient:
    BASE_URL = 'https://api.open-meteo.com/v1/forecast'

    def fetch_conditions(
        self,
        *,
        latitude: float,
        longitude: float,
        timezone: str,
        forecast_hours: int,
    ) -> dict:
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'timezone': timezone,
            'current': 'weather_code',
            'hourly': 'weather_code',
            'forecast_hours': max(1, forecast_hours),
            'daily': 'sunrise,sunset',
            'forecast_days': 2,
        }
        url = self.BASE_URL + '?' + urllib.parse.urlencode(params)

        try:
            with urllib.request.urlopen(url, timeout=20) as response:
                payload = json.loads(response.read().decode('utf-8'))
        except Exception as exc:  # pragma: no cover - network and service behavior
            raise OpenMeteoError(f'Failed to fetch weather data: {exc}') from exc

        if 'current' not in payload or 'daily' not in payload:
            raise OpenMeteoError('Weather data response is missing required fields.')

        return payload

    @staticmethod
    def is_blocking_weather(weather_code: int | None) -> bool:
        return weather_code in BLOCKING_WEATHER_CODES
