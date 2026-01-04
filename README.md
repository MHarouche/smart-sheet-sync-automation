# Smart Sheet Sync Automation

# Overview

A sophisticated two-job automation system that intelligently transfers records between Google Sheets based on status changes, applies business rules, and performs safe cleanup operations with timeout protection and edit conflict prevention.

# Key Features

Dual-Job Architecture: Separate sync and cleanup operations for optimal performance
Multi-Pass Cleanup Strategy: 5 scheduled passes with state persistence to handle large datasets
Edit Conflict Prevention: 60-second protection window prevents accidental deletions
Intelligent Categorization: Routes records to appropriate destinations based on configurable criteria
Comprehensive Logging: Automated email notifications with detailed execution summaries
Soft Lock Mechanism: Best-effort concurrency control with graceful degradation
Stateful Execution: Resumes from interruption points across multiple runs

# Technical Architecture

- Job 1: Sync & Transfer (Daily Execution)
  
Purpose: Evaluate source records and transfer qualified entries to destination sheets

Key Operations:

Scans source sheet for records meeting status criteria
Applies configurable business rules and exceptions
Routes records to appropriate destination tabs
Applies conditional defaults based on field values
Builds cleanup queue for Job 2
Sends immediate execution log

Performance: Fast execution (~1-2 minutes for typical datasets)

- Job 2: Cleanup & Delete (Multi-Pass Execution)
  
Purpose: Remove transferred records from source with timeout protection

Key Operations:

Processes deletion queue in manageable chunks
Respects recent edit protection window
Executes across multiple scheduled passes
Maintains state between executions
Sends single final log after completion

Performance: 5 passes with 4-minute runtime limit each (up to 20 minutes total)

# Why Two Separate Jobs?

# Performance Optimization

Transfer operations are lightweight (1-2 min)
Deletion operations are resource-intensive (up to 20 min)
Independent execution prevents bottlenecks


# Reliability

Transfer completes quickly, ensuring records are captured
Cleanup can retry without affecting transfer operations
Cascading failures are prevented

# Safety

Records are transferred before cleanup begins
Edit protection only needed during deletion
State persistence allows safe interruption/resumption

# Cleanup Strategy
Multi-Pass Design
Pass 1 (05:00) ─→ Process chunk, update state
                        ↓
Pass 2 (05:30) ─→ Resume from state, process next chunk
                        ↓
Pass 3 (06:00) ─→ Continue until complete OR
                        ↓
Pass 4 (06:30) ─→ timeout, then next pass
                        ↓
Pass 5 (07:00) ─→ Final attempt, send results email
Edit Protection
javascriptonEdit Trigger → Records timestamp per record ID
                        ↓
Cleanup evaluates: (current_time - edit_time) < 60s?
                        ↓
              Yes: Skip, retry next pass
              No:  Safe to delete
              

# Email Reporting
Job 1: Transfer Log (Immediate)
Subject: LOG - Record Transfer (SYNC) | [Date] | Added: X | Queue: Y
Contents:

Records added per destination tab
Total deletion queue size
Exception list with reasons
Evaluation summary

Frequency: Every execution (daily)
Job 2: Cleanup Log (On Completion)
Subject: LOG - Record Transfer (CLEANUP) | [Date] | Deleted: X | Not Found: Y
Contents:

Total deleted across all passes
Per-pass statistics (chunks, deleted, skipped, remaining)
Records not found (may have been manually deleted)
Edit-protected records with age information
Final status (Completed or Stopped at max passes)

# Frequency: Once per day (when cleanup finishes or max passes reached)

 # Performance Characteristics
Job 1 (Sync)

Typical Runtime: 1-2 minutes
Max Records/Run: ~10,000 (limited by sheet size)
Bottleneck: Sheet read/write operations

Job 2 (Cleanup)

Typical Runtime: 4-20 minutes (1-5 passes)
Throughput: ~200 deletions per minute
Bottleneck: Row deletion API calls

# Monitoring & Maintenance

- Health Indicators

Good Health:

Job 1 completes in <2 minutes
Job 2 completes by Pass 2-3
Zero persistent exceptions
All cleanup passes show decreasing remaining count

Needs Attention:

Job 1 timeouts
Job 2 requires all 5 passes regularly
Growing exception count
Increasing edit conflicts

- Maintenance Tasks

Weekly:

Review email logs for patterns
Verify queue clears by 07:00 daily
Check exception reasons

Monthly:

Analyze pass completion rates
Review business rule effectiveness
Update recipient list if needed

Quarterly:

Performance optimization review
Consider parameter tuning
Evaluate new feature requests

# Use Cases
This architecture is ideal for scenarios requiring:

Status-Based Workflows: Moving records through processing stages
Large-Scale Transfers: Handling datasets that exceed single-run limits
Edit-Safe Operations: Preventing conflicts with concurrent manual edits
Audit Requirements: Comprehensive logging of all operations
Resilient Processing: Automatic recovery from interruptions


# Technology Stack: Google Apps Script (JavaScript)
Pattern: Event-Driven Architecture with State Persistence
Complexity: Advanced (Multi-pass, stateful, concurrent)
Lines of Code: ~850
Test Coverage: Integration tests via manual execution
