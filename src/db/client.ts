import { serviceClient } from "./service-client";

export { serviceClient } from "./service-client";

export function getDb() {
  return serviceClient;
}

export function closeDb() {
  // no-op with Supabase
}
