import type { Config } from 'tailwindcss';
const config: Config = {
  content: ['./app/**/*.{ts,tsx}', './components/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: { bg: '#0b0b0d', card: '#141418', text: '#ffffff', subtle: '#9aa0a6', accent: '#6ee7ff' },
      boxShadow: { soft: '0 6px 24px rgba(0,0,0,0.35)' },
      borderRadius: { xl: '16px', '2xl': '20px' },
      fontFamily: {
        sans: ['var(--font-sans)', 'Inter', 'system-ui', 'sans-serif']
      }
    }
  },
  plugins: []
};
export default config;
