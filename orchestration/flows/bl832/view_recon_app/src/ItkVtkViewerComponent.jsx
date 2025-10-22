/**
 * ItkVtkViewerComponent.jsx
 *
 * A React component that creates an iframe, measures its internal body height
 * on load, and resizes the iframe to match it. Optionally sets the iframe's
 * background to "none" (only if same-origin).
 *
 * Props:
 *   @param {string} src       - The iframe's URL or data.
 * @return {JSX.Element} A rendered <div> containing an optional label and an auto-resizing iframe.
 */

import React, { useRef } from 'react';

export default function ItkVtkViewerComponent({ src }) {
  const iframeRef = useRef(null);

  return (
    <div className='itk-vtk-viewer' style={{ height: "100%", flex: "1" }}>
      <iframe
        ref={iframeRef}
        src={src}
        type="text/html"
        allowFullScreen
        webkitallowfullscreen="true"
        mozallowfullscreen="true"
        loading="eager"
        style={{
          width: '100vw',
          height: '100%',
          border: 'none',
        }}
      />
    </div>
  );
}
