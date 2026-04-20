import secrets, hashlib, base64
from flask import current_app

# PKCE helper
def generate_pkce():
    code_verifier = secrets.token_urlsafe(64)[:128]
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode('utf-8')).digest()
    ).rstrip(b'=').decode('utf-8')
    return code_verifier, code_challenge

def build_auth_url(code_challenge, state):
    client_id = current_app.config['FITBIT_CLIENT_ID']
    redirect_uri = current_app.config['REDIRECT_URI']
    scope = "activity cardio_fitness electrocardiogram heartrate irregular_rhythm_notifications nutrition oxygen_saturation profile respiratory_rate settings sleep temperature weight"
    base = current_app.config.get('OAUTH2_AUTHORIZE_URL', "https://www.fitbit.com/oauth2/authorize")
    params = (
        f"?client_id={client_id}&response_type=code&code_challenge={code_challenge}"
        f"&code_challenge_method=S256"
        f"&scope={scope.replace(' ', '%20')}"
        f"&redirect_uri={redirect_uri}"
        f"&state={state}"
    )
    return base + params
