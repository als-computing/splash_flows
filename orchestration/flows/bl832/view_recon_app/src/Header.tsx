import React, { useState } from 'react';
import { Button } from './Button';
import 'bluesky-web/style.css';
import './header.css';
import { TiledWidget } from './TiledWidget';

export interface HeaderProps {
  logoUrl: string;
  title: string;
  fileName: string;
  /**
   * Callback that receives the selected file URL.
   */
  onSelect?: (file_url: string) => void;
}

export const Header = ({ logoUrl, title, fileName, onSelect }: HeaderProps) => {
  const [showTiledWidget, setShowTiledWidget] = useState(false);

  // Open the modal when "Select Dataset" is clicked.
  const handleSelectClick = () => {
    setShowTiledWidget(true);
  };

  // Close the modal.
  const handleCloseModal = () => {
    setShowTiledWidget(false);
  };

  // Callback from TiledWidget receives the file_url.
  const handleTiledWidgetSelect = (file_url: string) => {
    console.log("Header received file_url:", file_url);
    if (onSelect) {
      onSelect(file_url);
    }
    setShowTiledWidget(false);
  };

  return (
    <header>
      <div className="storybook-header">
        <div>
          <img src={logoUrl} className="header-logo" alt="Logo" />
          <h1>{title}</h1>
          <div className="header-file-name">{fileName}</div>
        </div>
        <div>
          <Button size="small" label="Select Dataset" onClick={handleSelectClick} />
        </div>
      </div>

      {/* Modal for TiledWidget */}
      {showTiledWidget && (
        <div className="modal-overlay">
          <div className="modal-content">
            <div className="modal-header">
              <h3>Select a Dataset</h3>
              <button className="close-button" onClick={handleCloseModal}>
                ×
              </button>
            </div>
            <div className="modal-body">
              <div className="tiled-container">
                <TiledWidget onSelect={handleTiledWidgetSelect} />
              </div>
            </div>
          </div>
        </div>
      )}
    </header>
  );
};
