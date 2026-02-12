# DigitalOcean Deploy

This repo is now ready for DigitalOcean App Platform using:
- `Dockerfile`
- `.do/app.yaml`

## 1) Push code to GitHub
App Platform deploys from a git repo.

## 2) Update app spec
Edit `.do/app.yaml`:
- Replace `YOUR_GITHUB_USERNAME/YOUR_REPO`
- Set a real `FLASK_SECRET`
- Optionally change `branch`, region, and instance size

## 3) Deploy with doctl
```bash
doctl apps create --spec .do/app.yaml
```

For updates:
```bash
doctl apps update <APP_ID> --spec .do/app.yaml
```

## Notes
- Your app listens on `PORT=8080` in DigitalOcean.
- JSON data files are created at runtime by `app.py` if missing.
- Use HTTPS URL from App Platform for gamepad support in browser.
