import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.SANDBOX_SUPABASE_URL!;
const supabaseKey = process.env.SANDBOX_SUPABASE_KEY!;

export const supabase = createClient(supabaseUrl, supabaseKey);
