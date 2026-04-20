# Fitbit API Flask Integration

## Setup
- Copy `.env.example` to `.env` and fill in your keys
- Install dependencies: `pip install -r requirements.txt`
- Init DB: `python manage.py createdb`

## Run (development)
`python manage.py` (defaults to `HOST=127.0.0.1`, `PORT=3000`)

Visit `http://localhost:3000/` to start Fitbit OAuth.

## Production
- Set `FLASK_ENV=production` and a strong `SECRET_KEY`.
- Set `ALLOWED_ORIGINS` to a comma-separated list of your SPA origins (required).
- Set `FITBIT_CLIENT_ID`, `FITBIT_CLIENT_SECRET`, and `REDIRECT_URI` to your public HTTPS callback URL.
- Prefer PostgreSQL (`DATABASE_URL`) over SQLite if you use multiple app instances.
- Run behind Gunicorn, not the Flask dev server: `gunicorn -c gunicorn.conf.py wsgi:application`
- If the app sits behind a reverse proxy or load balancer, set `TRUST_PROXY_HEADERS=1` so `request.is_secure` and URL generation respect `X-Forwarded-*` headers.
- **No in-process polling:** the old 50-minute background sync thread has been removed. Use **Fitbit Subscription API** webhooks (`POST /api/fitbit/webhook`), **`POST /api/fitbit/sync`** from the app, or an external scheduler.
- **Continuous sync:** Register `https://<your-host>/api/fitbit/webhook` as the subscriber URL in [dev.fitbit.com/apps](https://dev.fitbit.com/apps) (JSON). Set **`FITBIT_SUBSCRIBER_VERIFICATION_CODE`** to the value Fitbit shows when you verify the subscriber (GET `?verify=` must return **204** for the correct code, **404** for the wrong one). After each successful OAuth, the server calls Fitbit **create subscription** (all collections) so new readings trigger a **204** response and a background sync to **`user_vitals`**. Optional **`FITBIT_SUBSCRIBER_ID`** if you use a non-default subscriber. Re-run **`POST /api/fitbit/register-subscriptions`** with `{"cognitoUserId":"..."}` after changing URLs or tokens.
- Set `VITALS_REALTIME_GATEWAY_SECRET` in production when using the realtime gateway; `/docs` and `/debug/*` are disabled unless you explicitly enable them with `ENABLE_DOCS` / `ENABLE_DEBUG_DB_VIEWS`.
- Use `GET /health` for load balancer health checks.
