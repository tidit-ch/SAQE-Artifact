use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct UdfArgDetail {
    pub name: String,
    pub description: String,
    pub data_type: String,
}

#[derive(Serialize, Clone)]
pub struct UdfDetail {
    pub name: String,
    pub category: String,
    pub description: String,
    pub syntax_example: String,
    pub sql_example: Option<String>,
    pub arguments: Vec<UdfArgDetail>,
}

pub trait UdfDoc {
    fn details(&self) -> &'static UdfDetail;
}
