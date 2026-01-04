/**
 * Shared Person Matching Utility for AppFolio Import
 * Last updated: 2026-01-04
 * 
 * CRITICAL MATCHING RULES:
 * 1. Email ALWAYS wins over phone if both match
 * 2. If email matches person_A and phone matches person_B, choose email and record conflict
 * 3. If neither matches, match_method="none" and match_value=""
 * 4. All matching is deterministic and explainable
 */

/**
 * Normalize email: trim + lowercase
 */
export function normalizeEmail(email) {
  if (!email || typeof email !== 'string') return '';
  return email.trim().toLowerCase();
}

/**
 * Normalize phone to E.164 format
 * - Strip all non-digits
 * - If 10 digits, assume US and prefix +1
 * - If 11 digits starting with 1, prefix +
 * - Otherwise return null
 */
export function normalizePhone(phone) {
  if (!phone || typeof phone !== 'string') return { raw: '', e164: null };
  
  const raw = phone;
  const digitsOnly = phone.replace(/\D/g, '');
  
  if (digitsOnly.length === 10) {
    return { raw, e164: `+1${digitsOnly}` };
  }
  
  if (digitsOnly.length === 11 && digitsOnly.startsWith('1')) {
    return { raw, e164: `+${digitsOnly}` };
  }
  
  // Invalid format - keep raw but no e164
  return { raw, e164: null };
}

/**
 * Match a person using email/phone with deterministic priority
 * 
 * @param {Object} params
 * @param {string} params.association_id - Association scope
 * @param {string} params.email - Raw email from input
 * @param {string} params.phone - Raw phone from input
 * @param {string} params.external_id - Optional external ID
 * @param {Map<string, string>} params.externalIdMap - external_id -> person_id
 * @param {Map<string, string>} params.emailMap - email_normalized -> person_id
 * @param {Map<string, string>} params.phoneMap - phone_e164 -> person_id
 * 
 * @returns {Object} Match result
 */
export function matchPerson({
  association_id,
  email,
  phone,
  external_id = null,
  externalIdMap = new Map(),
  emailMap = new Map(),
  phoneMap = new Map()
}) {
  const email_normalized = normalizeEmail(email);
  const { raw: phone_raw, e164: phone_e164 } = normalizePhone(phone);
  
  // Priority 1: External ID (if system uses it)
  if (external_id && externalIdMap.has(external_id)) {
    return {
      matched_person_id: externalIdMap.get(external_id),
      match_method: 'external_id',
      match_value: external_id,
      email_normalized,
      phone_raw,
      phone_e164: phone_e164 || '',
      status_details: ''
    };
  }
  
  // Priority 2: Email (ALWAYS wins over phone)
  const emailMatch = email_normalized ? emailMap.get(email_normalized) : null;
  const phoneMatch = phone_e164 ? phoneMap.get(phone_e164) : null;
  
  if (emailMatch && phoneMatch && emailMatch !== phoneMatch) {
    // CONFLICT: Email and phone match different people - choose email
    return {
      matched_person_id: emailMatch,
      match_method: 'email',
      match_value: email_normalized,
      email_normalized,
      phone_raw,
      phone_e164: phone_e164 || '',
      status_details: `Conflict: email matched ${emailMatch} but phone matched ${phoneMatch} (chose email)`
    };
  }
  
  if (emailMatch) {
    return {
      matched_person_id: emailMatch,
      match_method: 'email',
      match_value: email_normalized,
      email_normalized,
      phone_raw,
      phone_e164: phone_e164 || '',
      status_details: ''
    };
  }
  
  // Priority 3: Phone (only if no email match)
  if (phoneMatch) {
    return {
      matched_person_id: phoneMatch,
      match_method: 'phone',
      match_value: phone_e164,
      email_normalized,
      phone_raw,
      phone_e164: phone_e164 || '',
      status_details: ''
    };
  }
  
  // No match found
  return {
    matched_person_id: null,
    match_method: 'none',
    match_value: '',
    email_normalized,
    phone_raw,
    phone_e164: phone_e164 || '',
    status_details: `No match found (email=${email_normalized || 'none'}, phone=${phone_e164 || 'none'})`
  };
}

/**
 * Build lookup maps from Person records for efficient matching
 * 
 * @param {Array} persons - Array of Person records
 * @returns {Object} Maps for matching
 */
export function buildPersonLookupMaps(persons) {
  const emailMap = new Map();
  const phoneMap = new Map();
  const externalIdMap = new Map();
  
  for (const person of persons) {
    // Email map
    if (person.emails && Array.isArray(person.emails)) {
      for (const email of person.emails) {
        const normalized = normalizeEmail(email);
        if (normalized && !emailMap.has(normalized)) {
          emailMap.set(normalized, person.id);
        }
      }
    }
    
    // Phone map
    if (person.phones && Array.isArray(person.phones)) {
      for (const phone of person.phones) {
        const { e164 } = normalizePhone(phone);
        if (e164 && !phoneMap.has(e164)) {
          phoneMap.set(e164, person.id);
        }
      }
    }
    
    // External ID map (if applicable)
    if (person.external_id && !externalIdMap.has(person.external_id)) {
      externalIdMap.set(person.external_id, person.id);
    }
  }
  
  return { emailMap, phoneMap, externalIdMap };
}