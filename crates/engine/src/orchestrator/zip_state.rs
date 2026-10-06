//! ZIP workspace state: codec arbitration, staged sources, and the pending dump-message ledger.

use super::*;

pub(super) struct ZipState {
    pub(super) rendition: Rendition,
    pub(super) dir: PathBuf,
    pub(super) sources: Arc<Mutex<Vec<ZipTrackEntry>>>,
    pub(super) codec: Arc<Mutex<Option<String>>>,
    pub(super) generation_hash: Option<String>,
}

pub(super) fn set_primary_zip_error(ctx: &TaskContext, error: impl Into<String>) {
    let mut primary_zip_error = ctx
        .primary_zip_error
        .lock()
        .expect("primary ZIP error poisoned");
    if primary_zip_error.is_none() {
        *primary_zip_error = Some(error.into());
    }
}

pub(super) fn primary_zip_error(ctx: &TaskContext) -> Option<String> {
    ctx.primary_zip_error
        .lock()
        .expect("primary ZIP error poisoned")
        .clone()
}

pub(super) fn remember_zip_dump_message(ctx: &TaskContext, message_id: DumpMessageRef) {
    ctx.zip_new_dump_messages
        .lock()
        .expect("ZIP messages poisoned")
        .push(message_id);
}

pub(super) fn transfer_zip_dump_messages(ctx: &TaskContext, committed: &[DumpMessageRef]) {
    let mut pending = ctx
        .zip_new_dump_messages
        .lock()
        .expect("ZIP messages poisoned");
    for message_id in committed {
        if let Some(index) = pending
            .iter()
            .position(|pending_id| pending_id == message_id)
        {
            pending.remove(index);
        }
    }
}

pub(super) fn take_uncommitted_zip_dump_messages(ctx: &TaskContext) -> Vec<DumpMessageRef> {
    let mut pending = ctx
        .zip_new_dump_messages
        .lock()
        .expect("ZIP messages poisoned");
    std::mem::take(&mut *pending)
}

pub(super) fn archive_codec_replaced(replacement: Codec, existing: Codec) -> bool {
    match replacement {
        Codec::Alac | Codec::Aac => matches!(existing, Codec::Alac | Codec::Aac),
        other => existing == other,
    }
}

pub(super) fn seed_zip_codec(state: &ZipState, codec: Codec) {
    let rank = |codec: Codec| match codec {
        Codec::Alac => 3,
        Codec::Ec3 => 2,
        Codec::Aac => 1,
    };
    let mut current = state.codec.lock().expect("zip codec poisoned");
    if current
        .as_deref()
        .and_then(|value| value.parse::<Codec>().ok())
        .is_none_or(|existing| rank(codec) > rank(existing))
    {
        *current = Some(codec.as_str().to_owned());
    }
}

pub(super) fn codec_allowed_for_rendition(rendition: Rendition, codec: Codec) -> bool {
    match rendition {
        Rendition::Primary => matches!(codec, Codec::Alac | Codec::Aac),
        Rendition::Atmos => codec == Codec::Ec3,
    }
}
