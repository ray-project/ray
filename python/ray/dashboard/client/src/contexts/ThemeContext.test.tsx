import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { ThemeProvider, useTheme } from '../contexts/ThemeContext';

// Test component to verify theme context works
const TestComponent = () => {
  const { mode, toggleTheme } = useTheme();
  
  return (
    <div>
      <div data-testid="current-mode">{mode}</div>
      <button data-testid="toggle-button" onClick={toggleTheme}>
        Toggle Theme
      </button>
    </div>
  );
};

describe('ThemeContext', () => {
  const renderWithThemeProvider = (component: React.ReactElement) => {
    return render(
      <ThemeProvider>
        {component}
      </ThemeProvider>,
    );
  };

  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
  });

  it('should default to light mode', () => {
    renderWithThemeProvider(<TestComponent />);
    
    expect(screen.getByTestId('current-mode')).toHaveTextContent('light');
  });

  it('should toggle between light and dark modes', async () => {
    const user = userEvent.setup();
    renderWithThemeProvider(<TestComponent />);
    
    const toggleButton = screen.getByTestId('toggle-button');
    const currentModeDisplay = screen.getByTestId('current-mode');
    
    // Initially light
    expect(currentModeDisplay).toHaveTextContent('light');
    
    // Toggle to dark
    await user.click(toggleButton);
    expect(currentModeDisplay).toHaveTextContent('dark');
    
    // Toggle back to light
    await user.click(toggleButton);
    expect(currentModeDisplay).toHaveTextContent('light');
  });

  it('should persist theme preference in localStorage', async () => {
    const user = userEvent.setup();
    renderWithThemeProvider(<TestComponent />);
    
    const toggleButton = screen.getByTestId('toggle-button');
    
    // Toggle to dark
    await user.click(toggleButton);
    
    // Check localStorage was updated
    expect(localStorage.getItem('ray-dashboard-theme')).toBe('dark');
  });

  it('should restore theme from localStorage', () => {
    // Set localStorage to dark before rendering
    localStorage.setItem('ray-dashboard-theme', 'dark');
    
    renderWithThemeProvider(<TestComponent />);
    
    expect(screen.getByTestId('current-mode')).toHaveTextContent('dark');
  });
});