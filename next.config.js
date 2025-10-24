/** @type {import('next').NextConfig} */
const SUPABASE_HOST = new URL(process.env.NEXT_PUBLIC_SUPABASE_URL).host;

const nextConfig = {
  reactStrictMode: true,
  experimental: { serverActions: { bodySizeLimit: '2mb' } },
  images: {
    // keep your existing remote patterns
    remotePatterns: [
      { protocol: 'https', hostname: '**.supabase.co' },
      { protocol: 'https', hostname: 'lh3.googleusercontent.com' },
      { protocol: 'https', hostname: 'avatars.githubusercontent.com' },
      { protocol: 'https', hostname: 'media.licdn.com' },
    ],
    // add explicit domains so Next/Image is happy everywhere
    domains: [SUPABASE_HOST, 'lh3.googleusercontent.com', 'avatars.githubusercontent.com', 'media.licdn.com'],
  },
};

module.exports = nextConfig;
