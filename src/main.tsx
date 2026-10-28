import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import PackageGallery from './PackageGallery';
import './styles.css';

const packageMatch=window.location.pathname.match(/^\/package-view\/([A-Za-z0-9_-]+)$/);
createRoot(document.getElementById('root')!).render(<React.StrictMode>{packageMatch?<PackageGallery id={packageMatch[1]}/>:<App />}</React.StrictMode>);
