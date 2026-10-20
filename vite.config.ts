import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({ plugins: [react()], build: {emptyOutDir:false}, server: { host: '127.0.0.1', proxy: { '/api': `http://127.0.0.1:${process.env.PORT || '8786'}` } } });
