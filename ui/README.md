# ask-astro-ui

## Developing

Install dependencies:

```bash
npm install
```

Add backend API sever url in `ui/.env` file

```bash
ASK_ASTRO_API_URL=http://0.0.0.0:8080
```

Run the development server:

```bash
npm run dev

# or start the server and open the app in a new browser tab
npm run dev -- --open
```

## Building

To create a production version of your app:

```bash
npm run build
```

You can preview the production build with `npm run preview`.
