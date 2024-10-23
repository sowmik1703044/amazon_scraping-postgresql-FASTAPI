from transformers import GPT2Tokenizer, GPT2LMHeadModel, Trainer, TrainingArguments
from datasets import load_from_disk

# Load the preprocessed dataset
def fine_tune_gpt2():
    print("Loading preprocessed data...")
    dataset = load_from_disk("preprocessed_data")

    # Load tokenizer and model
    tokenizer = GPT2Tokenizer.from_pretrained("gpt2")
    tokenizer.pad_token = tokenizer.eos_token  # GPT-2 doesn't have a pad token, so we set it to eos_token

    model = GPT2LMHeadModel.from_pretrained("gpt2")

    # Tokenize the dataset
    def tokenize_function(examples):
        tokenized_inputs = tokenizer(examples['input_text'], padding="max_length", truncation=True, max_length=512)
        tokenized_inputs['labels'] = tokenized_inputs['input_ids']  # Set labels for loss calculation
        return tokenized_inputs

    tokenized_dataset = dataset.map(tokenize_function, batched=True)

    # Define training arguments
    training_args = TrainingArguments(
        output_dir="./results",
        evaluation_strategy="epoch",  # Changed to 'epoch' for consistency
        save_strategy="epoch",        # Saving model at each epoch
        num_train_epochs=3,
        per_device_train_batch_size=4,
        per_device_eval_batch_size=4,
        logging_dir="./logs",
        logging_steps=10,
        save_total_limit=2,
        load_best_model_at_end=True,  # Now both save and evaluation happen on 'epoch'
        report_to="none"
    )

    # Initialize Trainer
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_dataset["train"],
        eval_dataset=tokenized_dataset["test"],
    )

    # Train the model
    print("Starting training...")
    trainer.train()

if __name__ == "__main__":
    fine_tune_gpt2()
