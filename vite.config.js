import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

export default defineConfig({
  plugins: [solid()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://127.0.0.1:8000"
    },
    allowedHosts: ["3579-115-186-228-99.ngrok-free.app"]
  },
  build: {
    target: "esnext"
  }
});
