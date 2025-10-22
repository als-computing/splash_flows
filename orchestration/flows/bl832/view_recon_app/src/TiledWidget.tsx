import React, { useRef, useEffect } from 'react';
import { Tiled } from 'bluesky-web';
import 'bluesky-web/style.css';

export interface TiledWidgetProps {
  /**
   * Callback that receives the computed file URL when the visible "Select" button is clicked.
   */
  onSelect?: (file_url: string) => void;
}

/**
 * TiledWidget wraps the Tiled widget and continuously attaches a custom click handler to the currently
 * visible "Select" button. When clicked, it extracts metadata from the DOM by looking for an <h3>
 * with class "text-sky-950" immediately followed by a <div> containing a <pre> with JSON metadata.
 * It parses that JSON, extracts the first ancestor from attributes.ancestors, builds the file URL,
 * and calls onSelect.
 */
export const TiledWidget = ({ onSelect }: TiledWidgetProps) => {
  const containerRef = useRef<HTMLDivElement>(null);
  // Store the currently attached button and its listener.
  const currentButtonRef = useRef<HTMLButtonElement | null>(null);
  const currentHandleRef = useRef<((event: Event) => void) | null>(null);

  // Helper function to extract metadata from the DOM.
  const extractMetadata = (): any | null => {
    const container = containerRef.current;
    if (!container) {
      console.warn("extractMetadata: Container not available.");
      return null;
    }
    // Use an adjacent sibling selector to locate the div immediately following the metadata header.
    const metaContainer = container.querySelector('h3.text-sky-950 + div');
    if (!metaContainer) {
      console.warn("extractMetadata: Adjacent div after metadata header not found.");
      return null;
    }
    console.log("extractMetadata: Found metadata container:", metaContainer.outerHTML);
    // Look for a <pre> element within this container.
    const preElem = metaContainer.querySelector('pre');
    if (!preElem) {
      console.warn("extractMetadata: <pre> element not found inside metadata container.");
      return null;
    }
    const text = preElem.textContent?.trim();
    if (!text) {
      console.warn("extractMetadata: No text content in <pre> element.");
      return null;
    }
    if (text.startsWith("{") && text.endsWith("}")) {
      try {
        const parsed = JSON.parse(text);
        console.log("extractMetadata: Successfully parsed metadata:", parsed);
        return parsed;
      } catch (error) {
        console.error("extractMetadata: Failed to parse metadata JSON:", error);
        return null;
      }
    } else {
      console.warn("extractMetadata: Metadata content is not valid JSON:", text);
      return null;
    }
  };

  useEffect(() => {
    const container = containerRef.current;
    if (!container) {
      console.warn("TiledWidget: container ref not set.");
      return;
    }

    let attached = false;

    // Attach a click listener to the given button.
    const attachListener = (button: HTMLButtonElement) => {
      const handleClick = (event: Event) => {
        event.preventDefault();
        console.log("Visible 'Select' button clicked");
        const meta = extractMetadata();
        let file_url = "";
        if (meta && meta.attributes?.ancestors && Array.isArray(meta.attributes.ancestors) && meta.attributes.ancestors.length > 0) {
          const fileId = meta.attributes.ancestors[0];
          file_url = `http://localhost:8000/zarr/v2/${fileId}`;
          console.log("Extracted file_url:", file_url);
        } else {
          console.warn("extractMetadata: Metadata missing or ancestors array empty.");
        }
        onSelect && onSelect(file_url);
      };
      button.addEventListener("click", handleClick);
      currentButtonRef.current = button;
      currentHandleRef.current = handleClick;
    };

    // Detach the click listener.
    const detachListener = (button: HTMLButtonElement, handle: (event: Event) => void) => {
      button.removeEventListener("click", handle);
    };

    // Find the currently visible "Select" button.
    const getVisibleSelectButton = (): HTMLButtonElement | undefined => {
      const buttons = Array.from(container.querySelectorAll("button"));
      return buttons.find(
        btn => btn.textContent?.trim() === "Select" && btn.offsetParent !== null
      );
    };

    // Set up a MutationObserver to monitor DOM changes.
    const observer = new MutationObserver(() => {
      const newButton = getVisibleSelectButton();
      if (!newButton) {
        if (currentButtonRef.current && currentHandleRef.current) {
          console.log("MutationObserver: No visible 'Select' button found. Removing listener.");
          detachListener(currentButtonRef.current, currentHandleRef.current);
          currentButtonRef.current = null;
          currentHandleRef.current = null;
        }
        return;
      }
      if (currentButtonRef.current !== newButton) {
        if (currentButtonRef.current && currentHandleRef.current) {
          console.log("MutationObserver: Visible 'Select' button changed. Removing listener from previous button.");
          detachListener(currentButtonRef.current, currentHandleRef.current);
        }
        console.log("MutationObserver: Attaching listener to new visible 'Select' button:", newButton.outerHTML);
        attachListener(newButton);
      }
    });

    observer.observe(container, { childList: true, subtree: true });
    console.log("MutationObserver started on container.");

    // Cleanup on unmount.
    return () => {
      observer.disconnect();
      if (currentButtonRef.current && currentHandleRef.current) {
        detachListener(currentButtonRef.current, currentHandleRef.current);
      }
    };
  }, [onSelect]);

  return (
    <div ref={containerRef}>
      <Tiled />
    </div>
  );
};
