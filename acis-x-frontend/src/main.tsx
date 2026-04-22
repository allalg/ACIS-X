import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import '@fontsource/dm-mono'
import '@fontsource/geist'
import '@fontsource/jetbrains-mono'
import { Toaster } from 'sonner'
import { EventStreamProvider } from './contexts/EventStreamContext'
import './index.css'
import App from './App.tsx'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 3,
      retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 15000),
      refetchOnWindowFocus: false,
    },
  },
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <EventStreamProvider>
          <App />
          <Toaster position="bottom-right" visibleToasts={3} duration={5000} />
        </EventStreamProvider>
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
)
