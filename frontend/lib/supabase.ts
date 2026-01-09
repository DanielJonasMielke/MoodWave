import { createClient } from "@supabase/supabase-js";

const supabaseUrl = process.env.NEXT_PUBLIC_SANDBOX_SUPABASE_URL!;
const supabaseKey = process.env.NEXT_PUBLIC_SANDBOX_SUPABASE_KEY!;

export const supabase = createClient(supabaseUrl, supabaseKey);
