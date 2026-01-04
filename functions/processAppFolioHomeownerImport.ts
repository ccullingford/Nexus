// Last updated: 2026-01-04
import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';
import { matchPerson, buildPersonLookupMaps, normalizeEmail, normalizePhone } from './_shared/person_matching.js';

/**
 * Process AppFolio Homeowner Directory Import
 * 
 * CRITICAL RULES:
 * - Email ALWAYS wins over phone matching
 * - All matches are deterministic and explainable
 * - Snapshot rows include full matching context
 * - NO Owner entity - only Person + UnitRole
 */

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user || user.role !== 'admin') {
      return Response.json({ error: 'Forbidden: Admin access required' }, { status: 403 });
    }
    
    const { import_job_id } = await req.json();
    
    if (!import_job_id) {
      return Response.json({ error: 'import_job_id is required' }, { status: 400 });
    }
    
    // Get import job
    const importJob = await base44.asServiceRole.entities.ImportJob.get(import_job_id);
    if (!importJob) {
      return Response.json({ error: 'Import job not found' }, { status: 404 });
    }
    
    // Extract association_id from column_mappings or metadata
    const association_id = importJob.column_mappings?.association_id || importJob.metadata?.association_id;
    if (!association_id) {
      return Response.json({ error: 'association_id not found in import job' }, { status: 400 });
    }
    
    // Update job status to running
    await base44.asServiceRole.entities.ImportJob.update(import_job_id, {
      status: 'running',
      started_at: new Date().toISOString()
    });
    
    // Fetch CSV data
    const csvResponse = await fetch(importJob.file_url);
    const csvText = await csvResponse.text();
    const rows = parseCSV(csvText);
    
    if (rows.length === 0) {
      await base44.asServiceRole.entities.ImportJob.update(import_job_id, {
        status: 'completed',
        completed_at: new Date().toISOString(),
        total_rows: 0,
        processed_rows: 0,
        error_summary: 'No rows found in CSV'
      });
      return Response.json({ message: 'No rows to process' });
    }
    
    // Load all persons for this association
    const persons = await base44.asServiceRole.entities.Person.filter({ 
      association_id,
      status: 'active'
    });
    
    // Build lookup maps
    const { emailMap, phoneMap, externalIdMap } = buildPersonLookupMaps(persons);
    
    // Load all units for this association
    const units = await base44.asServiceRole.entities.Unit.filter({ association_id });
    const unitMap = new Map(units.map(u => [u.unit_number?.toLowerCase(), u.id]));
    
    // Statistics
    let matched_by_email = 0;
    let matched_by_phone = 0;
    let matched_none = 0;
    let conflict_count = 0;
    const conflicts = [];
    
    // Process each row
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const rowNumber = i + 1;
      
      try {
        // Extract fields from row
        const unit_number = row.unit || row.unit_number || row.property_address;
        const email = row.email || row.homeowner_email || '';
        const phone = row.phone || row.homeowner_phone || '';
        const first_name = row.first_name || row.homeowner_first_name || '';
        const last_name = row.last_name || row.homeowner_last_name || '';
        
        // Resolve unit
        const unit_id = unitMap.get(unit_number?.toLowerCase());
        if (!unit_id) {
          // Create review issue for missing unit
          await base44.asServiceRole.entities.DataReviewIssue.create({
            association_id,
            type: 'unit_create_required',
            import_job_id,
            import_row_id: null, // Will be set after snapshot creation
            reasons: [`Unit ${unit_number} not found`],
            snapshot: { row, rowNumber }
          });
          continue;
        }
        
        // Match person
        const matchResult = matchPerson({
          association_id,
          email,
          phone,
          external_id: null,
          externalIdMap,
          emailMap,
          phoneMap
        });
        
        // Update statistics
        if (matchResult.match_method === 'email') {
          matched_by_email++;
        } else if (matchResult.match_method === 'phone') {
          matched_by_phone++;
        } else if (matchResult.match_method === 'none') {
          matched_none++;
        }
        
        if (matchResult.status_details.includes('Conflict')) {
          conflict_count++;
          if (conflicts.length < 10) {
            conflicts.push({
              rowNumber,
              email: matchResult.email_normalized,
              phone: matchResult.phone_e164,
              details: matchResult.status_details
            });
          }
        }
        
        // Create or update person if no match
        let person_id = matchResult.matched_person_id;
        if (!person_id && (first_name || last_name || email || phone)) {
          const newPerson = await base44.asServiceRole.entities.Person.create({
            association_id,
            first_name,
            last_name,
            display_name: `${first_name} ${last_name}`.trim() || 'Unnamed',
            emails: matchResult.email_normalized ? [matchResult.email_normalized] : [],
            phones: matchResult.phone_e164 ? [matchResult.phone_e164] : [],
            created_from_source: 'appfolio'
          });
          person_id = newPerson.id;
        }
        
        // Create or update UnitRole (NOT Owner)
        if (person_id && unit_id) {
          const existingRoles = await base44.asServiceRole.entities.UnitRole.filter({
            unit_id,
            person_id
          });
          
          if (existingRoles.length === 0) {
            await base44.asServiceRole.entities.UnitRole.create({
              association_id,
              unit_id,
              person_id,
              role: 'owner', // Default role, can be changed by admin
              status: 'current',
              is_primary: true,
              source_tags: ['appfolio_import']
            });
          }
        }
        
        // Create snapshot row
        await base44.asServiceRole.entities.AppFolioHomeownerRow.create({
          association_id,
          unit_id,
          import_job_id,
          row_number: rowNumber,
          raw: row,
          parsed: {
            first_name,
            last_name,
            email,
            phone,
            unit_number
          },
          resolved_person_ids: person_id ? [person_id] : [],
          match_method: matchResult.match_method,
          match_value: matchResult.match_value,
          email_normalized: matchResult.email_normalized,
          phone_raw: matchResult.phone_raw,
          phone_e164: matchResult.phone_e164,
          status_details: matchResult.status_details,
          needs_review: matchResult.match_method === 'none' || matchResult.status_details.includes('Conflict')
        });
        
      } catch (error) {
        console.error(`Error processing row ${rowNumber}:`, error.message);
        // Continue processing other rows
      }
    }
    
    // Log summary
    console.log('Import Summary:');
    console.log(`Total rows: ${rows.length}`);
    console.log(`Matched by email: ${matched_by_email}`);
    console.log(`Matched by phone: ${matched_by_phone}`);
    console.log(`No match: ${matched_none}`);
    console.log(`Conflicts: ${conflict_count}`);
    
    if (conflicts.length > 0) {
      console.log('First 10 conflicts:');
      conflicts.forEach(c => {
        console.log(`  Row ${c.rowNumber}: ${c.details}`);
      });
    }
    
    // Update job status
    await base44.asServiceRole.entities.ImportJob.update(import_job_id, {
      status: 'completed',
      completed_at: new Date().toISOString(),
      total_rows: rows.length,
      processed_rows: rows.length,
      created_records: matched_none,
      log: `Matched: ${matched_by_email} email, ${matched_by_phone} phone, ${matched_none} new. Conflicts: ${conflict_count}`
    });
    
    return Response.json({
      success: true,
      summary: {
        total_rows: rows.length,
        matched_by_email,
        matched_by_phone,
        matched_none,
        conflict_count
      }
    });
    
  } catch (error) {
    console.error('Import error:', error);
    return Response.json({ error: error.message }, { status: 500 });
  }
});

/**
 * Simple CSV parser
 */
function parseCSV(csvText) {
  const lines = csvText.split('\n').filter(line => line.trim());
  if (lines.length === 0) return [];
  
  const headers = lines[0].split(',').map(h => h.trim().toLowerCase().replace(/"/g, ''));
  const rows = [];
  
  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split(',').map(v => v.trim().replace(/"/g, ''));
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] || '';
    });
    rows.push(row);
  }
  
  return rows;
}