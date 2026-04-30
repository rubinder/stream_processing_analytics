import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/pinot': {
        target: 'http://localhost:8099',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/pinot/, '/query/sql'),
      },
    },
  },
})
