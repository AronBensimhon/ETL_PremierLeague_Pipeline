# Treetop ABA Therapy - Bot Conversation Flow

A sales qualification bot that guides prospects through an 8-step conversation flow to qualify leads for ABA therapy services.

---

## Overview

This bot automates the lead qualification process by collecting essential information, verifying eligibility based on location and age requirements, and gathering contact details for callback. It handles multiple conversation paths including therapy inquiries, career applications, and disqualification scenarios.

---

## Architecture

### High-Level Flow
```
┌─────────────────────────┐
│   INITIAL CONTACT       │
│   Bot greets visitor    │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   INTEREST CHECK        │
│   ABA therapy or career │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  LOCATION QUALIFICATION │
│  Verify serviceable zip │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   AGE VERIFICATION      │
│   State-specific limits │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   DIAGNOSIS CHECK       │
│   Autism diagnosis      │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  CONTACT COLLECTION     │
│  Name, phone, email     │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  INSURANCE INFO         │
│  Primary & secondary    │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  INTAKE PROCESS         │
│  Detailed demographics  │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  ✓ QUALIFIED LEAD       │
│  Ready for callback     │
└─────────────────────────┘
```

---

## Process Stages

### 1. Initial Contact
- Bot initiates conversation
- Friendly greeting and value proposition

### 2. Interest Check
- Determine if prospect is interested in ABA therapy
- Route career inquiries to separate flow
- Explain benefits if hesitant

### 3. Location Qualification
- Auto-detect or ask for zip code
- Validate against serviceable areas (10 states)
- **Disqualify** if outside service zone

### 4. Age Verification
- Collect child's age
- Check against state-specific requirements
- Age limits vary: 18 months - 21 years depending on state
- **Disqualify** if outside age range

### 5. Diagnosis Check
- Confirm autism diagnosis
- Offer evaluation assistance if needed

### 6. Contact Collection
- Gather phone number
- Collect name and email
- Build contact record

### 7. Insurance Information
- Primary insurance provider
- Secondary insurance (if applicable)
- Verify coverage compatibility

### 8. Intake Process (Optional)
- Child's full details (name, DOB)
- Home address
- Insurance documentation
- Preferred schedule
- Behavioral history
- Referral source

---

## Disqualification Points

The bot will end the conversation at these checkpoints:

| Stage | Reason | Action |
|-------|--------|--------|
| Location | Zip code not in service area | End conversation abruptly |
| Age | Child outside state age limits | End conversation abruptly |
| Interest | Not interested after explanation | End conversation politely |
| Behavior | Inappropriate language/content | End conversation immediately |

---

## Service Coverage

**States Served:** Arizona, Georgia, North Carolina, Oklahoma, Virginia, Texas, Colorado, Nevada, Utah, New Mexico

**Service Types:**
- In-home therapy
- In-school therapy  
- In-clinic therapy (select locations)

---

## Key Features

- ✅ Automated location detection
- ✅ State-specific age validation
- ✅ Insurance verification
- ✅ Multi-language support (Spanish)
- ✅ Career application routing
- ✅ HIPAA compliant
- ✅ Remembers user information (no re-asking)

---
