/**
 * App.jsx
 *
 * The main application component. Renders a top header bar and an iframe that
 * takes up the remaining vertical space in the window.
 *
 * Usage:
 *   <App />
 *
 * @return {JSX.Element} A full-page layout with a header and auto-resizing iframe.
 */

import React, { useState } from 'react';
import { Header } from './Header';
import ItkVtkViewerComponent from './ItkVtkViewerComponent';
import './App.css';

function App() {
  // Store the file URL in state so that it can be updated by Header.
  const [fileUrl, setFileUrl] = useState(
    'http://localhost:8000/zarr/v2/rec20240425_104614_nist-sand-30-100_27keV_z8mm_n2625'
  );

  // Extract filename safely.
  const fileName = fileUrl.split('/').pop() || '';

  // Callback that receives the new file URL from Header/TiledWidget.
  const handleSelect = (newFileUrl) => {
    console.log("App received new fileUrl:", newFileUrl);
    // Update state only if a new, non-empty URL is returned.
    if (newFileUrl && newFileUrl !== fileUrl) {
      setFileUrl(newFileUrl);
    }
  };

  // Build the iframe source URL.
  const iframeSrc = 'http://localhost:8082/?fileToLoad=' + fileUrl;

  return (
    <div id="app">
      <Header 
        logoUrl="/images/als_logo_wheel.png"
        title="Tomography Visualizer powered by itk-vtk-viewer"
        fileName={fileName}
        onSelect={handleSelect}
      />
      <ItkVtkViewerComponent
        src={iframeSrc}
        height="100%"
        flex="1"
      />
    </div>
  );
}

export default App;
