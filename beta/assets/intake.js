(() => {
  // Map each NoteForms form slug to field IDs
  const FIELD_MAP = {
    'beta-user-tracker-conciergesync-b4yxaf': {
      name:'cf891a43-1e3c-4403-886c-3a7550291a22', email:'aafd8404-ef8a-4b17-ab3d-15e2357e93a5'
    },
    'section-2-authorized-user-card-setup-conciergesync-xnkw5k': {
      name:'90fae08a-5750-47e6-9080-f8ac7c501f39', email:'0146e3fa-ca0c-4dd0-9bed-cf4142693f79'
    },
    'section-3-loyalty-program-overview-40ya0y': {
      name:'6fcce27d-c4ee-455a-a89d-0ff481d5d96f', email:'891daeda-3fce-4725-9133-8c301eebc237'
    },
    'section-4-transferable-points-programs-wp4ujy': {
      name:'9052d9fd-994b-4e0b-adc8-d0a44c563dbb', email:'3142ee5f-e390-475c-8c6b-c8285434188c'
    }
    // If you add a hidden UID field to any form, add: uid:'<FIELD_ID>' to that object.
  };

  const qs = new URLSearchParams(location.search);
  const email = qs.get('email') || localStorage.getItem('cs_email') || '';
  const first = qs.get('first') || localStorage.getItem('cs_first') || '';
  const last  = qs.get('last')  || localStorage.getItem('cs_last')  || '';
  const uid   = qs.get('uid')   || localStorage.getItem('cs_uid')   || '';
  const name  = [first, last].filter(Boolean).join(' ').trim();

  const iframe = document.getElementById('nf-embed') || document.querySelector('iframe');
  if (!iframe) return;

  const slug = iframe.dataset.formSlug;               // e.g. section-3-loyalty-program-overview-40ya0y
  const next = iframe.dataset.next || '';             // e.g. 08-section-4.html
  const fields = FIELD_MAP[slug] || {};

  // Build embed URL (prefer data-src; fall back to src)
  const base = iframe.dataset.src || iframe.src || `https://noteforms.com/forms/${slug}?embed=true&theme=dark`;
  const url = new URL(base, location.href);

  if (fields.name && name)   url.searchParams.set(fields.name,  name);
  if (fields.email && email) url.searchParams.set(fields.email, email);
  if (fields.uid && uid)     url.searchParams.set(fields.uid,   uid);
  url.searchParams.set('_ts', Date.now().toString()); // cache-bust

  iframe.src = url.toString();

  // Success → next page (preserve QS)
  window.addEventListener('message', (e) => {
    if (!String(e.origin).includes('noteforms.com')) return;
    const d = e.data;
    if (d?.type === 'form-submitted' && d?.form?.slug === slug && next) {
      location.href = `${next}${location.search}`;
    }
  });
})();
