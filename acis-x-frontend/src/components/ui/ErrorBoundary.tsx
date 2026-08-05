import { Component, type ErrorInfo, type ReactNode } from 'react';

interface Props {
  children: ReactNode;
  fallback?: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

/**
 * React Error Boundary — catches rendering errors in child components
 * and displays a recovery UI instead of a blank page.
 */
class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error('[ErrorBoundary] Caught error:', error, info);
  }

  handleRetry = (): void => {
    this.setState({ hasError: false, error: null });
  };

  render(): ReactNode {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback;
      }

      return (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            minHeight: '400px',
            gap: '16px',
            padding: '32px',
            color: 'var(--color-text-secondary, #94a3b8)',
            fontFamily: 'var(--font-sans, system-ui, sans-serif)',
          }}
        >
          <div
            style={{
              fontSize: '48px',
              lineHeight: 1,
              opacity: 0.5,
            }}
          >
            ⚠️
          </div>
          <h2
            style={{
              margin: 0,
              fontSize: '18px',
              fontWeight: 600,
              color: 'var(--color-text-primary, #e2e8f0)',
            }}
          >
            Something went wrong
          </h2>
          <p
            style={{
              margin: 0,
              fontSize: '14px',
              textAlign: 'center',
              maxWidth: '400px',
            }}
          >
            {this.state.error?.message || 'An unexpected error occurred.'}
          </p>
          <button
            onClick={this.handleRetry}
            style={{
              padding: '8px 20px',
              fontSize: '14px',
              fontWeight: 500,
              border: '1px solid var(--color-border, #334155)',
              borderRadius: '8px',
              background: 'var(--color-surface, #1e293b)',
              color: 'var(--color-text-primary, #e2e8f0)',
              cursor: 'pointer',
              transition: 'all 0.15s ease',
            }}
          >
            Try Again
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}

export default ErrorBoundary;
