use std::collections::HashMap;

use crate::wire::ShortStr;

#[derive(Debug, Default)]
pub struct ConsumerTagMap {
    tag_to_id: HashMap<Vec<u8>, aurum_types::ConsumerId>,
    id_to_tag: HashMap<u64, Vec<u8>>,
    next_consumer_id: u64,
}

impl ConsumerTagMap {
    pub fn insert(&mut self, tag: ShortStr) -> aurum_types::ConsumerId {
        let key = tag.as_bytes().to_vec();
        if let Some(&id) = self.tag_to_id.get(&key) {
            return id;
        }
        self.next_consumer_id += 1;
        let id = aurum_types::ConsumerId(self.next_consumer_id);
        self.tag_to_id.insert(key.clone(), id);
        self.id_to_tag.insert(self.next_consumer_id, key);
        id
    }

    pub fn get(&self, tag: &ShortStr) -> Option<aurum_types::ConsumerId> {
        self.tag_to_id.get(tag.as_bytes()).copied()
    }

    pub fn tag_for(&self, id: aurum_types::ConsumerId) -> Option<ShortStr> {
        self.id_to_tag
            .get(&id.0)
            .map(|b| ShortStr::try_from_bytes(b).expect("tag"))
    }

    pub fn first_consumer_id(&self) -> Option<aurum_types::ConsumerId> {
        self.id_to_tag.keys().next().map(|&id| aurum_types::ConsumerId(id))
    }

    pub fn remove(&mut self, tag: &ShortStr) -> Option<aurum_types::ConsumerId> {
        let key = tag.as_bytes().to_vec();
        let id = self.tag_to_id.remove(&key)?;
        self.id_to_tag.remove(&id.0);
        Some(id)
    }
}
