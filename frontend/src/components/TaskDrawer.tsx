import { TransferPanel } from './TransferPanel'
import { useWorkbenchStore } from '../store'

export function TaskDrawer() {
  const open = useWorkbenchStore((state) => state.taskDrawerOpen)
  return open ? <TransferPanel /> : null
}
