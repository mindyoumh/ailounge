# `components/ui/` — shadcn/ui Primitives

13 UI components, standard shadcn/ui with minimal customization.

## Component Reference

| Component | `"use client"` | Radix Dependency | Variants / Sub-components |
|-----------|---------------|------------------|---------------------------|
| **Button** | No | `@radix-ui/react-slot` | 5 variants (default, destructive, outline, secondary, ghost, link), 4 sizes (default, sm, lg, icon), `asChild` prop |
| **Card** | No | — | 7 sub-components: Card, CardHeader, CardTitle, CardDescription, CardContent, CardFooter |
| **Badge** | No | — | 4 variants (default, secondary, destructive, outline) via `cva` |
| **Input** | No | — | ForwardRef, standard HTML input props, `type` prop |
| **Select** | Yes | `@radix-ui/react-select` | 10 exports: Root, Group, Value, Trigger, Content, Label, Item, Separator, ScrollUpButton, ScrollDownButton |
| **Tabs** | Yes | `@radix-ui/react-tabs` | 3 exports: TabsList, TabsTrigger, TabsContent |
| **Toggle** | Yes | `@radix-ui/react-toggle` | 2 variants (default, outline), 3 sizes (default, sm, lg) via `cva` |
| **Separator** | Yes | `@radix-ui/react-separator` | Orientation prop: `horizontal` (default) or `vertical` |
| **Table** | No | — | 6 sub-components: Table, TableHeader, TableBody, TableRow, TableHead, TableCell |
| **Skeleton** | No | — | `Skeleton` (shimmer animation) + `CardSkeleton` + `StatCardSkeleton` composite loaders |
| **DropdownMenu** | Yes | `@radix-ui/react-dropdown-menu` | 16 exports: Root, Trigger, Content, Item, CheckboxItem, RadioItem, Label, Separator, Shortcut, Group, Portal, Sub, SubContent, SubTrigger, RadioGroup |
| **Command** | Yes | `cmdk` | Command dialog with search input, group labels, item list, empty state, `shouldFilter` prop |
| **Dialog** | Yes | `@radix-ui/react-dialog` | 5 exports: Root, Trigger, Portal, Overlay, Content (with animation) |

## Usage Notes

- All components import `cn()` from `@/lib/utils`
- Components without `"use client"` can be used in Server Components
- `"use client"` components use Radix primitives that require browser APIs (interactivity, portals, etc.)
- Semicolons on `"use client"` directives are inconsistent across files — match the file you're editing
