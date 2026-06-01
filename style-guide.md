# Scrob Style Guide

This guide defines the UI and UX standards for the Scrob project, ensuring consistency across all pages and components.

## 🎨 Glassmorphism & Colors

Scrob uses a "Premium Glass" design system that relies on backdrop blurs, translucent backgrounds, and subtle borders.

### Color Palette (Dark Mode)
- **Page Background**: `var(--color-bg-page)` (#09090b / `zinc-950`)
- **Card Background**: `var(--color-bg-card)` (#18181b / `zinc-900`)
- **Border Base**: `var(--color-border-base)` (#27272a / `zinc-800`)
- **Text Primary**: `var(--color-text-base)` (#f4f4f5 / `zinc-100`)
- **Accent**: Configurable via `data-accent` attribute on `<html>`. Default is Purple.

### Glass Card Standard
Apply the `.glass-card` class or use the following utility pattern:
- **Background**: `bg-zinc-900/85` (or `var(--color-glass-bg)`)
- **Blur**: `backdrop-blur-xl` or `backdrop-blur-2xl`
- **Border**: `border border-zinc-800/85`
- **Rounding**: `rounded-2xl` for small cards, `rounded-3xl` for sections and modals.

---

## 🪟 Modals & Dialogs

All modals should follow a consistent structure to ensure they feel like part of the same interface.

### Backdrop
- **Class**: `fixed inset-0 z-50 flex items-center justify-center p-4 bg-zinc-950/85 backdrop-blur-sm`
- **Behavior**: Clicking the backdrop should close the modal.

### Container
- **Class**: `bg-zinc-900/85 border border-zinc-800/85 rounded-3xl w-full max-w-2xl overflow-hidden shadow-2xl flex flex-col max-h-[90vh] backdrop-blur-xl`
- **Sizing**: Use `max-w-md` for small alerts, `max-w-2xl` for forms/wizard, `max-w-4xl` for large data views.

### Header
- **Structure**:
  ```html
  <div class="px-6 py-5 border-b border-zinc-800/50 flex items-center justify-between">
    <div>
      <h2 class="text-xl font-bold text-zinc-100">Title</h2>
      <p class="text-xs text-zinc-400 mt-0.5">Subtitle description.</p>
    </div>
    <button class="w-8 h-8 rounded-full bg-zinc-800 hover:bg-zinc-700 ...">
      <svg>...</svg>
    </button>
  </div>
  ```

### Body & Footer
- **Body**: `p-6 overflow-y-auto space-y-6 flex-1`
- **Footer**: `px-6 py-4 border-t border-zinc-800/50 bg-zinc-950/30 flex justify-end gap-3`

---

## 🔡 Typography

- **Body Text**: `Inter` (Sans-serif)
- **Headings**: `Plus Jakarta Sans` (Bold/Black)

### Heading Styles
- **Page Title**: `class="page-title"` (`text-3xl font-bold tracking-tight`)
- **Section Headline**: `class="section-headline"` (`text-2xl font-black tracking-tight` + blue left bar)
- **Group/Detail Label**: `class="group-label"` (`text-[10px] font-bold uppercase tracking-widest text-zinc-500`)

---

## 🔘 Interactive Elements

### Buttons
- **Primary**: `bg-blue-600 hover:bg-blue-500 text-white font-bold rounded-xl transition-all active:scale-95 shadow-lg shadow-blue-600/20`
- **Secondary**: `bg-zinc-800 hover:bg-zinc-700 text-zinc-300 font-bold rounded-xl border border-zinc-700 active:scale-95`
- **Ghost/Icon**: `p-2 text-zinc-500 hover:text-zinc-100 hover:bg-zinc-800 rounded-xl transition-all`
- **Danger**: `bg-red-600/20 hover:bg-red-600/30 text-red-400 border border-red-500/30 font-bold rounded-xl transition-all`

### Dropdowns & Selects
- **Trigger**: `bg-zinc-950 border border-zinc-800 rounded-xl px-3 py-2 text-zinc-300 text-xs focus:border-blue-500 focus:outline-none transition cursor-pointer appearance-none`
- **Menu Container**: `bg-zinc-900/95 border border-zinc-800/85 rounded-2xl shadow-2xl backdrop-blur-xl ring-1 ring-white/10 overflow-hidden`
- **Menu Item**: `block w-full text-left px-4 py-2 text-sm text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800 transition-colors`

### Inputs & Forms
- **Text Input**: `bg-zinc-950/60 border border-zinc-800 focus:border-zinc-700 rounded-xl px-4 py-3 text-sm text-zinc-100 placeholder:text-zinc-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20 transition-all`
- **Checkbox**: `w-5 h-5 rounded-lg border-zinc-800 bg-zinc-900 text-blue-600 focus:ring-blue-500 transition-all cursor-pointer`
- **Label**: `block text-xs font-bold text-zinc-400 uppercase tracking-widest mb-2`

---

## 📱 Mobile Considerations

- **Touch Targets**: Minimum `44px` (approx `h-11`) for buttons and links.
- **Font Size**: Minimum `16px` for inputs to prevent iOS auto-zoom.
- **Safe Areas**: Use `.safe-area-top`, `.safe-area-bottom`, and `.sticky-top-safe`.
